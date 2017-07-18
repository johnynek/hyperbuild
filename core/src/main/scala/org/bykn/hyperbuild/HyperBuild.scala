package org.bykn.hyperbuild

import cats.Monoid
import cats.implicits._

sealed trait HyperBuild[M[_], +T] {
  import HyperBuild.{Ap, Const, Evented, Flatten, Fold}

  final def foldLeftM[U: Serialization](init: Build[M, U])(fn: Build[M, ((U, T)) => M[U]]): HyperBuild[M, U] =
    Fold[M, U, T](init, this, fn, implicitly)

  final def lookBack(delta: Int): HyperBuild[M, T] = this match {
    case Ap(fn, a, ser) => Ap(fn.lookBack(delta), a.lookBack(delta), ser)
    case Const(t) => Const(t)
    case Evented(ev, init, fn) => Evented(ev.lookBack(delta), init, fn)
    case Flatten(ma) => Flatten(ma.lookBack(delta))
    case Fold(init, changes, fn, ser) => Fold(init, changes.lookBack(delta), fn, ser)
  }

}

object HyperBuild {

  def evented[M[_], U](ev: Event, init: Build[M, U])(fn: Build[M, Timestamp => U]): HyperBuild[M, U] =
    Evented(ev, init, fn)

  def const[M[_], T](b: Build[M, T]): HyperBuild[M, T] =
    Const(b)

  implicit final class InvariantHyperBuild[M[_], A](val build: HyperBuild[M, A]) extends AnyVal {
    /**
     * Return the most recent event timestamp earlier than t and the build at that time
     * along with a Fingerprint of the entire build history up to an including the result
     * timestamp
     */
    final def at(t: Timestamp, memo: Memo[M]): (Option[Timestamp], M[(A, Fingerprint)]) = {
      import memo.monadError

      build match {
        case Ap(fn, a, ser) =>
          val (tsa, mfn) = fn.at(t, memo)
          val (tsb, ma) = a.at(t, memo)
          val tsc = Monoid[Option[Timestamp]].combine(tsa, tsb)
          val res = (mfn, ma)
            .map2 { case ((fn, ffn), (in, fa)) =>
              val fp = Fingerprint.combineAll(Fingerprint("apply") :: ffn :: fa :: Nil)
              memo.getOrElseUpdate[A](fp, ser)(monadError.pure(fn(in)))
                .map { case (a, _) =>
                  (a, fp)
                }
            }
            .flatten
          (tsc, res)
        case Const(b) =>
          (None, b.run(memo))
        case Evented(ev, init, fn) =>
          ev.mostRecent(None, t) match {
            case (Some(ts), fp) =>
              // need to combine fingerprint, with the evented history
              val maf = fn(ts).run(memo)
              (Some(ts), maf.map { case (a, fp2) => (a, Fingerprint.combine(fp, fp2)) })
            case (None, fp) =>
              // need to combine fingerprint, with the evented history
              val maf = init.run(memo)
              (None, maf.map { case (a, fp2) => (a, Fingerprint.combine(fp, fp2)) })
          }
        case Flatten(hb) =>
          val (ts, mma) = hb.at(t, memo)
          (ts, mma.flatMap { case (ma, fp) =>
            ma.map((_, Fingerprint.combine(Fingerprint("flatten"), fp)))
          })
        case Fold(init, changes, bfn, ser) =>
          def loop(ts: Timestamp): (Option[Timestamp], M[(A, Fingerprint)]) = {
            val (prev, mbf) = changes.at(ts, memo)
            val mpfp = HasFingerprint.fingerprint[M, Option[Timestamp]](prev)
            val afp = (init.run(memo), mpfp, mbf, bfn.run(memo))
              .map4 { case ((a, fa), fprev, (b, fb), (fn, ffn)) =>
                /**
                 * since fb is the full hash of history up to and including prev, this
                 * is a safe hash to key the fold function on
                 */
                val fp = Fingerprint.combineAll(Fingerprint("fold") :: fa :: fprev :: fb :: ffn :: Nil)
                memo.getOrElseUpdate[A](fp, ser) {
                  // this is lazy, we only loop if we have cache miss
                  prev match {
                    case None => fn((a, b))
                    case Some(prevt) =>
                      loop(prevt)._2.flatMap { case (a, _) =>
                        fn((a, b))
                      }
                  }
                }
                .map { case (a, _) => (a, fp) } // discard the fingerprint
              }
              .flatten

            (prev, afp)
          }
          loop(t)
      }
    }

    final def valueAt(t: Timestamp, m: Memo[M]): M[A] = {
      import m.monadError
      at(t, m)._2.map(_._1)
    }
  }

  implicit class HyperBuildApply[M[_], A, B](val build: HyperBuild[M, A => B]) extends AnyVal {
    def apply(hb: HyperBuild[M, A])(implicit ser: Serialization[B]): HyperBuild[M, B] =
      Ap(build, hb, ser)
  }

  implicit class HyperBuildM[M[_], A](val build: HyperBuild[M, M[A]]) extends AnyVal {
    def flatten: HyperBuild[M, A] = Flatten(build)
  }

  private case class Ap[M[_], A, B](fn: HyperBuild[M, A => B], a: HyperBuild[M, A], ser: Serialization[B]) extends HyperBuild[M, B]
  private case class Const[M[_], A](build: Build[M, A]) extends HyperBuild[M, A]
  private case class Evented[M[_], U](ev: Event, init: Build[M, U], fn: Build[M, Timestamp => U]) extends HyperBuild[M, U]
  private case class Flatten[M[_], A](hb: HyperBuild[M, M[A]]) extends HyperBuild[M, A]
  private case class Fold[M[_], A, B](init: Build[M, A], changes: HyperBuild[M, B], fn: Build[M, ((A, B)) => M[A]], ser: Serialization[A]) extends HyperBuild[M, A]
}
