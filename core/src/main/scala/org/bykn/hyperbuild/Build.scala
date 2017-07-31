package org.bykn.hyperbuild

import cats._
import cats.implicits._
import java.io.File
import scala.spores._

sealed trait Build[M[_], +A] {
  import Build.{Apply, Flatten, /* Cached, CachedOrBuild, Const, Pure, */ Named}

  final def mapCached[B: Serialization](fn: Spore[A, B]): Build[M, B] =
    Apply(Build.mod[M].fn[A, B, Spore[A, B]](fn), this).cached

  final def named(name: String): Build[M, A] =
    Named(this, name)

  final def next[B: Serialization](fn: Spore[A, M[B]]): Build[M, B] =
    Flatten[M, B](Apply[M, A, M[B]](Build.mod[M].fn[A, M[B], Spore[A, M[B]]](fn), this)).cached

  final def zip[B](that: Build[M, B]): Build[M, (A, B)] = {
    val withB = Build.mod[M].fn[A, B => (A, B), Build.ZipFn[A, B]](Build.ZipFn[A, B]())
    Apply(Apply(withB, this), that)
  }

  // /**
  //  * Transform to a new Monad N
  //  */
  // final def transform[N[_]](f: M ~> N): Build[N, A] =
  //   this match {
  //     case Const(a, hfp) => Const(a, hfp.transform(f))
  //     case Apply(fn, a) => Apply(fn.transform(f), a.transform(f))
  //     case p@Pure(_, _) =>
  //       def go[A0, A1](p: Pure[M, A0, A1]): Build[N, A1] = {
  //         val leib: Build.Leibniz[A1, M[A0]] = p.eq
  //         val newL: Build.Leibniz[A1, N[A0]] = ???
  //         Pure(p.of.transform(f), newL)
  //         // or:
  //         // val b: Build[N, N[A0]] = p.of.transform(f).pureBuild
  //         // type B[T] = Build[N, T]
  //         // newL.flip.subst[B](b)
  //       }
  //       go(p)
  //     case Flatten(nested) => Flatten(nested.transform(f).map { ma => f(ma) })
  //     case Cached(inner, ser) => Cached(inner.transform(f), ser)
  //     case CachedOrBuild(fp, ser, a) => CachedOrBuild(f(fp), ser, () => a().transform(f))
  //     case Named(inner, name) => Named(inner.transform(f), name)
  //   }
}

object Build {

  /**
   * Some private serializable and stable functions
   */
  private case class ZipFn[A, B]() extends Function1[A, B => (A, B)] with Serializable {
    def apply(a: A) = { (b: B) => (a, b) }
  }
  private case class AndThen[A, B, C]() extends Function1[(A => B, B => C), A => C] with Serializable {
    def apply(fns: (A => B, B => C)): A => C = fns._1.andThen(fns._2)
  }
  private case class ConstFn[A, B]() extends Function1[B, A => B] with Serializable {
    def apply(b: B) = { (a: A) => b }
  }
  private case class ForkFn[A, B, C]() extends Function1[(A => B, A => C), A => (B, C)] with Serializable {
    def apply(tup: (A => B, A => C)): A => (B, C) = { (a: A) => (tup._1(a), tup._2(a)) }
  }
  private case class ApplyFn[A, B]() extends Function1[(A => B, A), B] with Serializable {
    def apply(tup: (A => B, A)): B = tup._1(tup._2)
  }

  implicit class BuildInvariant[M[_], A](val build: Build[M, A]) extends AnyVal {
    final def cached(implicit ser: Serialization[A]): Build[M, A] = build match {
      case Cached(of, _) =>
        // don't nest caching, but use the new serialization
        Cached(of, ser)
      case CachedOrBuild(fp, _, a) =>
        // don't nest caching, but use the new serialization
        CachedOrBuild(fp, ser, a)
      case other =>
        Cached(other, ser)
    }

    /** lift the value into the monad M */
    final def pureBuild: Build[M, M[A]] =
      Pure(build, implicitly[Build.Leibniz[M[A], M[A]]])

    final def run(memo: Memo[M]): M[(A, Fingerprint)] = {
      import memo.monadError

      def applyFP(fn: Fingerprint, arg: Fingerprint, depth: Int): Fingerprint =
        Fingerprint.combineAll(Fingerprint(s"apply/result:$depth") :: fn :: arg :: Nil)

      /**
       * We only cache function application here with some number of flattenings
       * so U => M[V] is not a general function but is purely some number of flattenings
       */
      def cache[T, U, V](fn: Build[M, T => U], a: Build[M, T], depth: Int, ser: Serialization[V])(lift: U => M[V]): M[(V, Fingerprint)] = {
        val mf = fn.run(memo)
        val ma = a.run(memo)
        mf.product(ma).flatMap { case ((fab, fabF), (a, aF)) =>
          val bF = applyFP(fabF, aF, depth)
          memo.getOrElseUpdate(bF, ser)(lift(fab(a))).map((_, bF))
        }
      }

      def deepFlatten[T, R](depth: Int, of: Build[M, T], ser: Serialization[R])(fn: T => M[R]): M[(R, Fingerprint)] =
        of match {
          case Apply(f, a) =>
            cache(f, a, depth, ser)(fn)
          case p@Pure(_, _) =>
            def go[B, T1 <: T](p: Pure[M, B, T1]): M[(R, Fingerprint)] = {
              type Reader[X] = X => M[R]
              val fnM: M[B] => M[R] = p.eq.subst[Reader](fn)
              deepFlatten[B, R](depth - 1, p.of, ser) { b: B => fnM(Monad[M].pure(b)) }
            }
            go(p)
          case Flatten(inner) =>
            deepFlatten(depth + 1, inner, ser)(_.flatMap(fn))
          case Named(inner, name) =>
            memo.runNamed(name) { deepFlatten(depth, inner, ser)(fn) }
          case c@Const(_, _) =>
            def go[T1 <: T](c: Const[M, T1]): M[(R, Fingerprint)] =
              for {
                fp <- c.fp.fingerprint(c.const)
                rfp = flattenFP(fp, depth)
                r <- memo.getOrElseUpdate(rfp, ser)(fn(c.const))
              } yield (r, rfp)

            go(c)
          case Cached(inner, _) =>
            // The outer-most serialization wins
            deepFlatten(depth, inner, ser)(fn)
          case cob@CachedOrBuild(_, _, _) =>
            def go[T1 <: T](cob: CachedOrBuild[M, T1]): M[(R, Fingerprint)] = {

              // This must be a def or lazy val since below it is in a call-by-name
              def mt: M[T1] =
                cob.mfp.flatMap { fp =>
                  memo.getOrElseUpdate(fp, cob.ser)(cob.a().cached(cob.ser).run(memo).map(_._1))
                }

              cob.mfp.flatMap { fp =>
                val rfp = flattenFP(fp, depth)

                memo.getOrElseUpdate(rfp, ser)(mt.flatMap(fn))
                  .map((_, rfp))
              }
            }
            go(cob)
        }

      build match {
        case Const(a, fp) =>
          fp.fingerprint(a).map((a, _))
        case Apply(fn, a) =>
          (fn.run(memo), a.run(memo)).map2 { case ((fab, fpAB), (a, fpA)) =>
            val bF = applyFP(fpAB, fpA, 0)
            // we have to re-run the function without a caching
            (fab(a), bF)
          }
        case p@Pure(_, _) =>
          def go[B, A1 <: A](p: Pure[M, B, A1]): M[(A, Fingerprint)] =
            for {
              aFp <- p.of.run(memo)
              (a, fp) = aFp
              flatFp = flattenFP(fp, -1) // there is -1 flattening
            } yield (p.eq.flip.subst[Id](Monad[M].pure(a)), flatFp)
          go(p)
        case Flatten(m) =>
          for {
            maFp <- m.run(memo)
            (ma, fp) = maFp
            flatFp = flattenFP(fp, 1) // there is 1 flattening
            a <- ma
          } yield (a, flatFp)
        case Named(inner, name) =>
          memo.runNamed(name)(inner.run(memo))
        case Cached(other, ser) =>
          deepFlatten(0, other, ser)(monadError.pure)
        case CachedOrBuild(mfp, sera, a) =>
          mfp.flatMap { fp =>
            memo.getOrElseUpdate(fp, sera)(a().cached(sera).run(memo).map(_._1))
              .map((_, fp))
          }
      }
    }
  }

  implicit class BuildM[M[_], A](val build: Build[M, M[A]]) extends AnyVal {
    def flatten: Build[M, A] = Flatten(build)
    def flatCached(implicit ser: Serialization[A]): Build[M, A] =
      Cached(Flatten(build), ser)
  }

  implicit class BuildFn[M[_], A, B](val fn: Build[M, A => B]) extends AnyVal {
    def apply(a: Build[M, A]): Build[M, B] = Apply(fn, a)
    def apply(a: A)(implicit hfp: HasFingerprint[M, A]): Build[M, B] =
      apply(Const(a, hfp))

    def andThen[C](bfn: Build[M, B => C]): Build[M, A => C] =
      Apply(Const(AndThen[A, B, C](), HasFingerprint.serializable), fn.zip(bfn))
  }

  private sealed trait Leibniz[A, B] {
    def subst[F[_]](f: F[A]): F[B]
    final def flip: Leibniz[B, A] = {
      type F[T] = Leibniz[T, A]
      subst[F](Leibniz.refl[A])
    }
  }
  private object Leibniz {
    implicit def refl[A]: Leibniz[A, A] =
      new Leibniz[A, A] {
        @inline def subst[F[_]](f: F[A]): F[A] = f
      }
  }

  private case class Apply[M[_], A, B](fn: Build[M, A => B], a: Build[M, A]) extends Build[M, B]
  private case class Cached[M[_], A](b: Build[M, A], ser: Serialization[A]) extends Build[M, A]
  private case class CachedOrBuild[M[_], A](mfp: M[Fingerprint], ser: Serialization[A], a: () => Build[M, A]) extends Build[M, A]
  private case class Const[M[_], A](const: A, fp: HasFingerprint[M, A]) extends Build[M, A]
  private case class Pure[M[_], A, B](of: Build[M, A], eq: Leibniz[B, M[A]]) extends Build[M, B]
  private case class Flatten[M[_], A](nested: Build[M, M[A]]) extends Build[M, A]
  private case class Named[M[_], A](b: Build[M, A], name: String) extends Build[M, A]

  /**
   * Note, map and pure assume serializable. Kind of junky... sorry
   */
  implicit def app[M[_]]: Applicative[({type B[T] = Build[M, T]})#B] =
    new Applicative[({type B[T] = Build[M, T]})#B] {
      def pure[A](a: A): Build[M, A] =
        mod[M].const(a.asInstanceOf[A with Serializable])(HasFingerprint.serializable)

      def ap[A, B](fn: Build[M, A => B])(a: Build[M, A]): Build[M, B] =
        Apply(fn, a)
    }

  implicit def buildHasFingerprint[M[_]: Applicative, A]: HasFingerprint[M, Build[M, A]] = {
    def go[B](b: Build[M, B]): M[Fingerprint] = b match {
      case Apply(f, a) =>
        (go(f), go(a)).map2 { (ff, fa) =>
          Fingerprint.combineAll(Fingerprint("apply") :: ff :: fa :: Nil)
        }
      case CachedOrBuild(mfp, _, _) => mfp
      case const@Const(_, _) =>
        def run[B1 <: B](c: Const[M, B1]) = c.fp.fingerprint(c.const)
        run(const)
      case Pure(of, _) =>
        go(of).map { n => Fingerprint.combineAll(Fingerprint("pure") :: n :: Nil) }
      case Flatten(nested) => go(nested).map { n => Fingerprint.combineAll(Fingerprint("flatten") :: n :: Nil) }
      case Named(b, _) => go(b) // naming does not change the result
      case Cached(c, _) => go(c) // serialization does not change the values
    }

    HasFingerprint.fromM[M, Build[M, A]](go _)
  }

  def mod[M[_]]: Module[M] = new Module[M]

  private def flattenFP(fp: Fingerprint, depth: Int): Fingerprint =
    Fingerprint.combineAll(Fingerprint(s"flatten/result:$depth") :: fp :: Nil)

  final class Module[M[_]] {

    val unit: Build[M, Unit] =
      const(())

    val None: Build[M, Option[Nothing]] =
      const(scala.None)(HasFingerprint.serializable)

    def noneM[T]: Build[M, M[Option[T]]] =
      (None: Build[M, Option[T]]).pureBuild

    def const[A](a: A)(implicit hfp: HasFingerprint[M, A]): Build[M, A] =
      Const[M, A](a, hfp)

    /**
     * Create a constant value with a constructed Fingerprint
     * you must make sure these are actually globally unique
     * and change any time A changes
     */
    def constWithFingerprint[A](a: A, fp: Fingerprint): Build[M, A] =
      const(a)(HasFingerprint.const(fp))

    def constM[A](ma: M[A])(implicit M: Monad[M], hfp: HasFingerprint[M, A]): Build[M, A] = {
      val hfma = hfp.onM[M[A]](identity[M[A]] _).andThen(flattenFP(_, 1))
      Flatten(Const(ma, hfma))
    }

    def applyFn[A, B]: Build[M, ((A => B, A)) => B] =
      fn[(A => B, A), B, ApplyFn[A, B]](ApplyFn())

    def constFn[A, B](b: Build[M, B]): Build[M, A => B] =
      fn[B, A => B, ConstFn[A, B]](ConstFn())(b)

    private[hyperbuild] def cacheOrBuild[A](mfp: M[Fingerprint], bld: => Build[M, A])(implicit ser: Serialization[A]): Build[M, A] =
      CachedOrBuild(mfp, ser, () => bld)

    def fn[A, B, T <: Function1[A, B] with Serializable](f: T): Build[M, A => B] =
      const[Function1[A, B] with Serializable](f)(HasFingerprint.serializable[M, Function1[A, B] with Serializable])

    def fork[A, B, C](fn1: Build[M, A => B], fn2: Build[M, A => C]): Build[M, A => (B, C)] =
      fn[(A => B, A => C), A => (B, C), ForkFn[A, B, C]](ForkFn())(fn1.zip(fn2))

    /**
     * Assume we can serialize the function
     */
    def unsafeFn[A, B](f: A => B): Build[M, A => B] =
      fn[A, B, Function1[A, B] with Serializable](f.asInstanceOf[Function1[A, B] with Serializable])

    def failed[E](error: E)(implicit m: MonadError[M, E]): Build[M, Nothing] = {
      val hfp = HasFingerprint.fromM[M, Nothing](_ => m.raiseError(error))

      constM(m.raiseError(error))(m, hfp)
    }

    def source(fileName: String)(implicit me: MonadError[M, Throwable]): Build[M, File] =
      const(new File(fileName))

    def sources(files: Set[String])(implicit me: MonadError[M, Throwable]): Build[M, Set[File]] =
      files.toList.traverseU(source(_)).map(_.toSet)

    def dependencies(b: Build[M, _]): List[String] = b match {
      case Const(a, _) => Nil
      case Apply(fn, a) => dependencies(fn) ++ dependencies(a)
      case Pure(nested, _) => dependencies(nested)
      case Flatten(nested) => dependencies(nested)
      case Cached(b, _) => dependencies(b)
      case CachedOrBuild(_, _, fn) => dependencies(fn())
      case Named(inner, name) => name :: dependencies(inner)
    }
  }
}
