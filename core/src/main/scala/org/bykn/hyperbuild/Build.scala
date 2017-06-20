package org.bykn.hyperbuild

import cats._
import cats.implicits._
import java.io.File
import scala.spores._
import com.twitter.bijection.JavaSerializationInjection

sealed trait Build[M[_], A] {
  import Build.{Apply, Flatten, Keyed, Cached, Pure, Named}

  final def run(memo: Memo[M]): M[(A, Option[Fingerprint])] = {
    import memo.monadError

    /**
     * We only cache function application here
     */
    def cache[T, U, V](fn: Build[M, T => U], a: Build[M, T], ser: Serialization[V])(lift: U => M[V]): M[(V, Fingerprint)] = {
      val mf = fn.run(memo)
      val ma = a.run(memo)
      mf.product(ma).flatMap { case ((fab, fabF), (a, aF)) =>
        val input = fabF.map2(aF)(Fingerprint.combine _)

        input match {
          case None =>
            // no key, so we can't cache, but we can fingerprint
            for {
              u <- monadError.catchNonFatal(fab(a))
              a <- lift(u)
              fp = Fingerprint.of(a)(ser)
            } yield (a, fp)
          case Some(fpKey) =>
            memo.getOrElseUpdate(fpKey, ser)(lift(fab(a)))
        }
      }
    }

    def fingerprint[T](mt: M[T], prefix: Fingerprint)(implicit hp: HasFingerprint[M, T]): M[(T, Fingerprint)] =
      for {
        t <- mt
        f <- hp.fingerprint(t)
        total = Fingerprint.combine(prefix, f)
      } yield (t, f)

    def deepFlatten[T, R](depth: Int, of: Build[M, T], ser: Serialization[R])(fn: T => M[R]): M[(R, Fingerprint)] = {
      def prefix(str: String): String = s"flatten_$depth/$str"
      def fprefix(str: String): Fingerprint = Fingerprint(prefix(str))

      of match {
        case Apply(f, a) =>
          cache(f, a, ser)(fn)
        case Flatten(inner) =>
          deepFlatten(depth + 1, inner, ser)(_.flatMap(fn))
        case Keyed(toKey, _) =>
          // caching supersedes keying.
          deepFlatten(depth, toKey, ser)(fn)
        case Named(inner, name) =>
          memo.runNamed(name) { deepFlatten(depth, inner, ser)(fn) }
        case Pure(t) =>
          // can't cache, but we can produce a fingerprint
          fingerprint(fn(t), fprefix("pure"))(HasFingerprint.fromSer(ser))
        case Cached(inner, innerSer) =>
          // reset the caching, but update the fingerprint
          for {
           topt <- deepFlatten(0, inner, innerSer)(monadError.pure)
           r <- fn(topt._1)
           fp = Fingerprint.combine(fprefix("cached"), topt._2)
          } yield (r, fp)
      }
    }

    def opt(m: M[(A, Fingerprint)]): M[(A, Option[Fingerprint])] =
      m.map { case (a, fp) => (a, Some(fp)) }

    this match {
      case Pure(a) =>
        // Can't create use the cache without a serialization
        monadError.pure((a, None))
      case Apply(fn, a) =>
        // Can't create use the cache without a serialization
        // but we can pass through a fingerprint by hashing the
        // inputs since A => B is a pure function
        (fn.run(memo) |@| a.run(memo)).map { case ((fab, fAB), (a, fA)) =>
          val resultFP = (fAB |@| fA)
            .map { (x, y) => Fingerprint.combineAll(Fingerprint("apply/result") :: x :: y :: Nil) }

          (fab(a), resultFP)
        }
      case Flatten(m) =>
        // We can't create a fingerprint without a serialization
        for {
          maofp <- m.run(memo)
          ma = maofp._1
          a <- ma
        } yield (a, None)
      case Keyed(of, fp) =>
        val ma = of.run(memo).map(_._1)
        opt(fingerprint(ma, Fingerprint("keyed"))(fp))
      case Named(inner, name) =>
        memo.runNamed(name)(inner.run(memo))
      case Cached(other, ser) =>
        opt(deepFlatten(0, other, ser)(monadError.pure))
    }
  }

  final def mapCached[B: Serialization](fn: Spore[A, B]): Build[M, B] =
    Apply(Build.mod[M].keyFn(fn), this).cached

  final def cached(implicit ser: Serialization[A]): Build[M, A] = this match {
    case Cached(of, _) =>
      // don't nest caching, but use the new serialization
      Cached(of, ser)
    case other =>
      Cached(other, ser)
  }

  final def toKey(implicit fp: HasFingerprint[M, A]): Build[M, A] = this match {
    case Keyed(of, _) =>
      Keyed(of, fp)
    case notKeyed =>
      Keyed(notKeyed, fp)
  }

  final def keyBy[B](fn: A => B)(implicit fp: HasFingerprint[M, B]): Build[M, A] =
    toKey(fp.on(fn))

  final def keyByM[B](fn: A => M[B])(implicit fp: HasFingerprint[M, B], m: Monad[M]): Build[M, A] =
    toKey(fp.onM(fn))

  final def named(name: String): Build[M, A] =
    Named(this, name)

  final def next[B: Serialization](fn: Spore[A, M[B]]): Build[M, B] =
    Flatten[M, B](Apply[M, A, M[B]](Build.mod[M].keyFn(fn), this)).cached

  /**
   * Transform to a new Monad N, renaming all the
   * names to `name.suffix`
   */
  final def transform[N[_]](f: M ~> N): Build[N, A] =
    this match {
      case Pure(a) => Pure(a)
      case Apply(fn, a) => Apply(fn.transform(f), a.transform(f))
      case Flatten(nested) => Flatten(nested.transform(f).map { ma => f(ma) })
      case Cached(inner, ser) => Cached(inner.transform(f), ser)
      case Keyed(inner, fp) => Keyed(inner.transform(f), fp.transform(f))
      case Named(inner, name) => Named(inner.transform(f), name)
    }


}

object Build {

  implicit class BuildM[M[_], A](val build: Build[M, M[A]]) extends AnyVal {
    def flatten: Build[M, A] = Flatten(build)
    def flatCached(implicit ser: Serialization[A]): Build[M, A] =
    Cached(Flatten(build), ser)
  }

  case class Pure[M[_], A](pure: A) extends Build[M, A]
  case class Apply[M[_], A, B](fn: Build[M, A => B], a: Build[M, A]) extends Build[M, B] {
    type Init = A
  }
  case class Flatten[M[_], A](nested: Build[M, M[A]]) extends Build[M, A]
  case class Cached[M[_], A](b: Build[M, A], ser: Serialization[A]) extends Build[M, A]
  case class Keyed[M[_], A](b: Build[M, A], fp: HasFingerprint[M, A]) extends Build[M, A]
  case class Named[M[_], A](b: Build[M, A], name: String) extends Build[M, A]

  implicit def app[M[_]]: Applicative[({type B[T] = Build[M, T]})#B] =
    new Applicative[({type B[T] = Build[M, T]})#B] {
      def pure[A](a: A): Build[M, A] = Pure(a)
      def ap[A, B](fn: Build[M, A => B])(a: Build[M, A]): Build[M, B] =
        Apply(fn, a)

      // TODO: override product keep HasFingerprint and Serialization around
      // override def product[A, B](a: Build[M, A], b: Build[M, B]): Build[M, (A, B)] =
    }


  def mod[M[_]]: Module[M] = new Module[M]

  final class Module[M[_]] {
    def pure[A](a: A): Build[M, A] =
      Pure(a)

    def pureM[A](ma: M[A]): Build[M, A] =
      Flatten(Pure(ma))

    def key[A](a: A)(implicit fp: HasFingerprint[M, A]): Build[M, A] =
      Pure(a).toKey

    def keyFn[A, B](fn: Spore[A, B]): Build[M, A => B] = {
      val ser = new JavaSerializationInjection(fn.getClass.asInstanceOf[Class[Spore[A, B]]])
      app[M].widen(key(fn)(HasFingerprint.fromSer(ser)))
    }

    def failed[A, E](error: E)(implicit m: MonadError[M, E]): Build[M, A] =
      pureM(m.raiseError(error))

    def source(fileName: String)(implicit me: MonadError[M, Throwable]): Build[M, File] =
      pure(new File(fileName)).toKey

    def sources(files: Set[String])(implicit me: MonadError[M, Throwable]): Build[M, Set[File]] =
      files.toList.traverseU(source(_)).map(_.toSet)

    def dependencies(b: Build[M, _]): List[String] = b match {
      case Pure(a) => Nil
      case Apply(fn, a) => dependencies(fn) ++ dependencies(a)
      case Flatten(nested) => dependencies(nested)
      case Cached(b, _) => dependencies(b)
      case Keyed(of, _) => dependencies(of)
      case Named(inner, name) => name :: dependencies(inner)
    }
  }
}
