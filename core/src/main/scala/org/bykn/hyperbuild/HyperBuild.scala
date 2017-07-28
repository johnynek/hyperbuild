package org.bykn.hyperbuild

import cats.implicits._

sealed trait HyperBuild[M[_], +T] {
  import HyperBuild.Evented

  final def at(t: Timestamp): (Option[Timestamp], Build[M, T]) = this match {
    case Evented(ev, fn) =>
      val opt = ev.mostRecent(None, t)._1
      (opt, fn(opt))
  }

  final def foldLeftM[U: Serialization](init: Build[M, U])(foldFn: (U, T) => M[U]): HyperBuild[M, U] = this match {
    case Evented(ev, fn) =>
      def go(opt: Option[Timestamp]): Build[M, U] = opt match {
        case None =>
          // Here is the base case:
          val ba = fn(None)
          // TODO: map uses serialization, which captures foldFn. We really need a Fingerprint
          init.zip(ba).map { case (b, a) => foldFn(b, a) }.flatten.cached
        case s@Some(ts) =>
          val ba = fn(s)
          // get the previous timestamp to ts:
          val (prev, fp) = ev.mostRecent(None, ts)
          val fnFP = Fingerprint(fn.getClass.getName) // TODO is bullshit
          val resFP = Fingerprint.combine(fp, fnFP)
          // make sure to lazily loop so we don't unroll in the common case
          val bbprev = Build.mod[M].cacheOrBuild[U](resFP, go(prev))
          bbprev.zip(ba).map { case (b, a) => foldFn(b, a) }.flatten.cached
      }
      Evented[M, U](ev, go _)
  }

  final def shift(delta: Int): HyperBuild[M, T] = this match {
    case Evented(ev, fn) => Evented(ev.shift(delta), fn)
  }
}

object HyperBuild {

  def evented[M[_], U](ev: Event, init: Build[M, U])(fn: Timestamp => Build[M, U]): HyperBuild[M, U] =
    Evented(ev, {
      case None => init
      case Some(t) => fn(t)
    })

  def eventedM[M[_], U](ev: Event, init: Build[M, M[U]])(fn: Timestamp => Build[M, M[U]]): HyperBuild[M, U] =
    evented(ev, init)(fn).flatten

  implicit def const[M[_], T](b: Build[M, T]): HyperBuild[M, T] =
    Evented(Event.empty, _ => b)

  implicit class HyperBuildApply[M[_], A, B](val build: HyperBuild[M, A => B]) extends AnyVal {
    def apply(hb: HyperBuild[M, A]): HyperBuild[M, B] =
      (build, hb) match {
        case (Evented(e1, f1), Evented(e2, f2)) =>
          Evented(e1 merge e2, { optTs =>
            f1(optTs)(f2(optTs))
          })
      }

    def apply(hb: Build[M, A]): HyperBuild[M, B] =
      apply(const(hb))
  }

  implicit class HyperBuildM[M[_], A](val build: HyperBuild[M, M[A]]) extends AnyVal {
    def flatten: HyperBuild[M, A] = build match {
      case Evented(ev, fn) => Evented(ev, fn.andThen(_.flatten))
    }
  }

  implicit class InvariantHyperBuild[M[_], A](val build: HyperBuild[M, A]) extends AnyVal {
    def cached(implicit ser: Serialization[A]): HyperBuild[M, A] = build match {
      case Evented(ev, fn) => Evented(ev, fn.andThen(_.cached))
    }

    final def valueAt(t: Timestamp, m: Memo[M]): M[A] = {
      import m.monadError

      build.at(t)._2.run(m).map(_._1)
    }

  }

  private case class Evented[M[_], A](ev: Event, fn: Option[Timestamp] => Build[M, A]) extends HyperBuild[M, A]
}
