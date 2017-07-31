package org.bykn.hyperbuild

import cats.{Applicative, Monoid}
import cats.implicits._

sealed trait HyperBuild[M[_], +T] extends Serializable {
  import HyperBuild.{ Apply, Cached, Const, Evented, Flatten, FoldM }

  final def at(t: Timestamp)(implicit M: Applicative[M]): (Option[Timestamp], Build[M, T]) = this match {
    case Cached(of, ser) =>
      val (opt, b) = of.at(t)
      (opt, b.cached(ser))
    case Const(c) => (None, c)
    case Evented(ev, fn) =>
      val opt = ev.mostRecent(None, t)._1
      import Timestamp.optionTimestampHasFingerprint
      (opt, fn(opt))
    case Flatten(of) =>
      val (opt, b) = of.at(t)
      (opt, b.flatten)
    case FoldM(init, parts, fn, ser) =>
      val (last, fp) = fingerprintAt(t)
      last match {
        case None => (None, init)
        case s@Some(prevTs) =>
          // we have the fingerprint
          (s, Build.mod[M].cacheOrBuild[T](fp, {
            // This is call-by-name to lazily loop
            val bt = at(prevTs)._2
            val bu = parts.at(prevTs)._2
            fn(bt.zip(bu)).flatten.cached(ser)
          })(ser))
      }
    case Apply(fn, a) =>
      val (ts1, bfn) = fn.at(t)
      val (ts2, ba) = a.at(t)
      (Monoid[Option[Timestamp]].combine(ts1, ts2), bfn(ba))
  }

  final def fingerprintAt(t: Timestamp)(implicit M: Applicative[M]): (Option[Timestamp], M[Fingerprint]) = {
    def buildFp[A](b: Build[M, A]): M[Fingerprint] =
      HasFingerprint.fingerprint[M, Build[M, A]](b)

    this match {
      case Cached(of, _) => of.fingerprintAt(t)
      case Const(c) => (None, HasFingerprint.fingerprint[M, Build[M, T]](c))
      case Evented(ev, fn) =>
        val (optTs, fp) = ev.mostRecent(None, t)
        (optTs,
          buildFp(fn).map { fnFp =>
            Fingerprint.combineAll(Fingerprint("Evented") :: fp :: fnFp :: Nil)
          })
      case Flatten(of) =>
        val (optTs, mfp) = of.fingerprintAt(t)
        (optTs, mfp
          .map { fp => Fingerprint.combineAll(Fingerprint("Flatten") :: fp :: Nil) })
      case FoldM(init, parts, fn, ser) =>
        val (optTs, mfp) = parts.fingerprintAt(t)
        val initFp = buildFp(init)
        val fnFp = buildFp(fn)
        (optTs, (initFp, mfp, fnFp).map3 { (a, b, c) =>
          Fingerprint.combineAll(Fingerprint("FoldM") :: a :: b :: c :: Nil)
        })
      case Apply(fn, a) =>
        val (ts1, fp1) = fn.fingerprintAt(t)
        val (ts2, fp2) = a.fingerprintAt(t)
        (Monoid[Option[Timestamp]].combine(ts1, ts2),
          (fp1, fp2).map2 { (a, b) =>
            Fingerprint.combineAll(Fingerprint("Apply") :: a :: b :: Nil)
          })
    }
  }

  final def foldLeftM[U: Serialization](init: Build[M, U])(foldFn: Build[M, ((U, T)) => M[U]]): HyperBuild[M, U] =
    FoldM[M, U, T](init, this, foldFn, implicitly)

  final def shift(delta: Int): HyperBuild[M, T] = this match {
    case Evented(ev, fn) => Evented(ev.shift(delta), fn)
    case Cached(of, ser) => Cached(of.shift(delta), ser)
    case c@Const(_) => c
    case Flatten(of) => Flatten(of.shift(delta))
    case FoldM(init, stream, fn, ser) => FoldM(init, stream.shift(delta), fn, ser)
    case Apply(fn, a) => Apply(fn.shift(delta), a.shift(delta))
  }
}

object HyperBuild {

  def evented[M[_], A](ev: Event)(fn: Build[M, Option[Timestamp] => A]): HyperBuild[M, A] =
    Evented(ev, fn)

  private case class OrElse[A]() extends Function1[(A, Timestamp => A), Option[Timestamp] => A] with Serializable {
      def apply(in: (A, Timestamp => A)) = {
        case None => in._1
        case Some(t) => in._2(t)
      }
    }

  def evented[M[_], A](ev: Event, init: Build[M, A])(fn: Build[M, Timestamp => A]): HyperBuild[M, A] = {

    val orElse: Build[M, ((A, Timestamp => A)) => (Option[Timestamp] => A)] =
      Build.mod[M].fn[(A, Timestamp => A), Option[Timestamp] => A, OrElse[A]](OrElse())
    evented(ev)(orElse(init.zip(fn)))
  }

  implicit def const[M[_], T](b: Build[M, T]): HyperBuild[M, T] =
    Const(b)

  implicit class HyperBuildApply[M[_], A, B](val build: HyperBuild[M, A => B]) extends AnyVal {
    def apply(hb: HyperBuild[M, A]): HyperBuild[M, B] =
      Apply(build, hb)

    def apply(hb: Build[M, A]): HyperBuild[M, B] =
      apply(const(hb))
  }

  implicit class HyperBuildM[M[_], A](val build: HyperBuild[M, M[A]]) extends AnyVal {
    def flatten: HyperBuild[M, A] = Flatten(build)
  }

  implicit class InvariantHyperBuild[M[_], A](val build: HyperBuild[M, A]) extends AnyVal {
    final def valueAt(t: Timestamp, m: Memo[M]): M[A] = {
      import m.monadError

      build.at(t)._2.run(m).map(_._1)
    }

    final def cached(implicit ser: Serialization[A]): HyperBuild[M, A] =
      Cached(build, ser)
  }

  private case class Const[M[_], A](bld: Build[M, A]) extends HyperBuild[M, A]
  private case class Cached[M[_], A](of: HyperBuild[M, A], ser: Serialization[A]) extends HyperBuild[M, A]
  private case class Evented[M[_], A](ev: Event, fn: Build[M, Option[Timestamp] => A]) extends HyperBuild[M, A]
  private case class Flatten[M[_], A](of: HyperBuild[M, M[A]]) extends HyperBuild[M, A]
  private case class FoldM[M[_], A, B](init: Build[M, A], hb: HyperBuild[M, B], fold: Build[M, ((A, B)) => M[A]], ser: Serialization[A]) extends HyperBuild[M, A]
  private case class Apply[M[_], A, B](fn: HyperBuild[M, A => B], a: HyperBuild[M, A]) extends HyperBuild[M, B]
}
