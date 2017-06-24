package org.bykn.hyperbuild

import cats.{Applicative, Monoid}
import cats.implicits._

sealed trait HyperBuild[M[_], T] {
  import HyperBuild.{Evented, Fold, Ap, Flatten}

  // Return the most recent event and the build at that time
  def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(T, Option[Fingerprint])])

  final def foldLeftM[U](init: Build[M, U])(fn: (U, T) => M[U]): HyperBuild[M, U] =
    Fold(init, this, fn)

  final def lookBack(delta: Int): HyperBuild[M, T] = this match {
    case Evented(ev, init, fn) => Evented(ev.lookBack(delta), init, fn)
    case Ap(fn, a) => Ap(fn.lookBack(delta), a.lookBack(delta))
    case Flatten(ma) => Flatten(ma.lookBack(delta))
    case Fold(init, changes, fn) => Fold(init, changes.lookBack(delta), fn)
  }

  final def valueAt(t: Timestamp, m: Memo[M]): M[T] = {
    import m.monadError
    at(t, m)._2.map(_._1)
  }
}

object HyperBuild {

  def evented[M[_], T, U](ev: Event[T], init: Build[M, U])(fn: T => Build[M, U]): HyperBuild[M, U] =
    Evented(ev, init, fn)

  def const[M[_], T](b: Build[M, T]): HyperBuild[M, T] =
    evented[M, Nothing, T](Event.empty, b) { n => n: Build[M, T] }

  implicit class HyperBuildM[M[_], A](val build: HyperBuild[M, M[A]]) extends AnyVal {
    def flatten: HyperBuild[M, A] = Flatten(build)
  }

  private case class Fold[M[_], A, B](init: Build[M, A], changes: HyperBuild[M, B], fn: (A, B) => M[A]) extends HyperBuild[M, A] {
    def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(A, Option[Fingerprint])]) = {
      import memo.monadError

      def aser: Serialization[A] = ???
      // make a fingerprint(init, changes, fn, ts)
      def makeFP(ts: Timestamp): Fingerprint = ???

      def loop(ts: Timestamp): (Option[Timestamp], M[A]) = {
        val (prev, mbf) = changes.at(ts, memo)

        val mb: M[B] = mbf.map(_._1)
        val bfp: M[Option[Fingerprint]] = mbf.map(_._2)

        prev match {
          case None =>
            // This is the first one, init handles the caching here
            val ma = init.run(memo).map(_._1)
            (prev, ma.map2(mb)(fn).flatten)
          case s@Some(prev) =>
            // we may have already built this:
            // check the cache:
            val ma = memo.getOrElseUpdate[A](makeFP(prev), aser)(loop(prev)._2).map(_._1) // discard the fingerprint
            (s, ma)
        }
      }

      val (opt, ma) = loop(t)
      (opt, ma.map((_, Option.empty[Fingerprint]))) // TODO put a correct fingerprint here
    }
  }

  private case class Evented[M[_], T, U](ev: Event[T], init: Build[M, U], fn: T => Build[M, U]) extends HyperBuild[M, U] {
    def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(U, Option[Fingerprint])]) =
      ev.mostRecent(t) match {
        case Some((ts, t)) => (Some(ts), fn(t).run(memo))
        case None => (None, init.run(memo))
      }
  }

  private case class Flatten[M[_], A](hb: HyperBuild[M, M[A]]) extends HyperBuild[M, A] {
    def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(A, Option[Fingerprint])]) = {
      val (ts, mma) = hb.at(t, memo)
      // TODO maybe smart memoization here
      import memo.monadError
      (ts, mma.flatMap { case (ma, optFP) =>
        // TODO, we can probably compute the fingerprint of a in some smart way
        // maybe just hashing the string "flatten" with a.
        ma.map { a => (a, Option.empty) }
      })
    }
  }

  implicit def hyperBuildApplicative[M[_]]: Applicative[({type A[T] = HyperBuild[M, T]})#A] =
    new Applicative[({type A[T] = HyperBuild[M, T]})#A] {
      def pure[A](a: A): HyperBuild[M, A] =
        HyperBuild.const(Build.mod[M].pure(a))
      def ap[A, B](fn: HyperBuild[M, A => B])(a: HyperBuild[M, A]): HyperBuild[M, B] =
        Ap(fn, a)
    }

  private case class Ap[M[_], A, B](
    fn: HyperBuild[M, A => B],
    a: HyperBuild[M, A]) extends HyperBuild[M, B] {

    def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(B, Option[Fingerprint])]) = {
      import memo.monadError
      val (tsa, mfn) = fn.at(t, memo)
      val (tsb, ma) = a.at(t, memo)
      val tsc = Monoid[Option[Timestamp]].combine(tsa, tsb)
      // TODO we should cache at the given tsc this function application
      val bc = mfn.map(_._1).ap(ma.map(_._1))

      // TODO add a fingerprint
      (tsc, bc.map { b => (b, None) })
    }
  }
}
