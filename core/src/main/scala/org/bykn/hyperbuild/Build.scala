package org.bykn.hyperbuild

import cats._
import cats.implicits._
import java.io.File
import scala.spores._
import com.twitter.bijection.JavaSerializationInjection

sealed trait Build[M[_], A] {
  import Build.{Apply, Flatten, Cached, Pure}

  final def run(memo: Memo[M]): M[(A, Option[Fingerprint])] = {
    import memo.monadError

    def cache[T, U, V](key: String, fn: Build[M, T => U], a: Build[M, T], ser: Serialization[V])(lift: U => M[V]): M[(V, Fingerprint)] = {
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
            memo.getOrElseUpdate(key, fpKey, ser)(lift(fab(a)))
              .map { case (t, fp) => (t, fp) }
        }
      }
    }

    def deepFlatten[T, R](depth: Int, of: Build[M, T], ser: Serialization[R])(fn: T => M[R]): M[(R, Fingerprint)] = of match {
      case Apply(f, a) =>
        cache(s"Flatten_$depth/Apply", f, a, ser)(fn)
      case Flatten(inner) =>
        deepFlatten(depth + 1, inner, ser)(_.flatMap(fn))
      case Pure(t) =>
        // can't cache, but we can produce a fingerprint
        fn(t).map { a =>
          val fp0 = Fingerprint.of(a)(ser)
          // need to include the depth:
          val fp = Fingerprint.combine(Fingerprint(s"Flatten_$depth/Pure"), fp0)
          (a, fp)
        }
      case Cached(inner, innerSer) =>
        for {
         topt <- deepFlatten(0, inner, innerSer)(monadError.pure)
         r <- fn(topt._1)
         fp = Fingerprint.combine(Fingerprint(s"Flatten_$depth/Cached"), topt._2)
        } yield (r, fp)
    }

    this match {
      case Pure(a) =>
        // Can't create use the cache without a serialization
        monadError.pure((a, None))
      case Apply(fn, a) =>
        // Can't create use the cache without a serialization
        val mf = fn.run(memo).map(_._1)
        val ma = a.run(memo).map(_._1)
        mf.ap(ma).map { (_, None) }
      case Flatten(m) =>
        // We can't create a fingerprint without a serialization
        for {
          maofp <- m.run(memo)
          ma = maofp._1
          a <- ma
        } yield (a, None)
      case Cached(other, ser) =>
        deepFlatten(0, other, ser)(monadError.pure)
          .map { case (a, fp) =>
            (a, Some(fp))
          }
    }
  }

  final def mapCached[B: Serialization](fn: Spore[A, B]): Build[M, B] =
    Apply(Build.cachedFn[M, A, B](fn), this).cached

  final def cached(implicit ser: Serialization[A]): Build[M, A] =
    Cached(this, ser)

  final def next[B: Serialization](fn: Spore[A, M[B]]): Build[M, B] =
    Flatten[M, B](Apply[M, A, M[B]](Build.cachedFn[M, A, M[B]](fn), this)).cached

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

  implicit def app[M[_]]: Applicative[({type B[T] = Build[M, T]})#B] =
    new Applicative[({type B[T] = Build[M, T]})#B] {
      def pure[A](a: A): Build[M, A] = Pure(a)
      def ap[A, B](fn: Build[M, A => B])(a: Build[M, A]): Build[M, B] =
        Apply(fn, a)
    }

  def pure[M[_], A](a: A): Build[M, A] =
    Pure(a)

  def pureM[M[_], A](ma: M[A]): Build[M, A] =
    Flatten(Pure(ma))

  def key[M[_], A: Serialization](a: A): Build[M, A] =
    Pure(a).cached

  def cachedFn[M[_], A, B](fn: Spore[A, B]): Build[M, A => B] =
    app[M].widen(Pure(fn).cached(new JavaSerializationInjection(fn.getClass.asInstanceOf[Class[Spore[A, B]]])))

  def failed[M[_], A, E](error: E)(implicit m: MonadError[M, E]): Build[M, A] =
    pureM[M, A](m.raiseError(error))

  def source[M[_]: Monad](fileName: String): Build[M, File] =
    pure(new File(fileName))

  def sources[M[_]: Monad](files: Set[String]): Build[M, Set[File]] =
    files.toList.traverseU(source[M](_)).map(_.toSet)

  def roots[M[_]](b: Build[M, _]): List[_] = b match {
    case Pure(a) => List(a)
    case Apply(fn, a) => roots(fn) ++ roots(a)
    case Flatten(nested) => roots(nested)
    case Cached(b, _) => roots(b)
  }
}
