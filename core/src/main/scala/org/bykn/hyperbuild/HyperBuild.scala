package org.bykn.hyperbuild

import cats.Applicative
import cats.implicits._

sealed trait HyperBuild[M[_], T] {
  def at(t: Timestamp): Build[M, T]

  final def runAt(t: Timestamp, m: Memo[M]): M[T] = {
    import m.monadError
    at(t).run(m).map(_._1)
  }
}

object HyperBuild {

  def evented[M[_], T, U](ev: Event[T], init: Build[M, U])(fn: T => Build[M, U]): HyperBuild[M, U] =
    Evented(ev, init, fn)

  def const[M[_], T](b: Build[M, T]): HyperBuild[M, T] =
    evented[M, Nothing, T](Event.empty, b) { n => n: Build[M, T] }

  private case class Evented[M[_], T, U](ev: Event[T], init: Build[M, U], fn: T => Build[M, U]) extends HyperBuild[M, U] {
    def at(t: Timestamp) = ev.mostRecent(t) match {
      case Some((_, t)) => fn(t)
      case None => init
    }
  }

  implicit def hyperBuildApplicative[M[_]]: Applicative[({type A[T] = HyperBuild[M, T]})#A] =
    new Applicative[({type A[T] = HyperBuild[M, T]})#A] {
      def pure[A](a: A): HyperBuild[M, A] =
        HyperBuild.const(Build.mod[M].pure(a))
      def ap[A, B](fn: HyperBuild[M, A => B])(a: HyperBuild[M, A]): HyperBuild[M, B] =
        Map2[M, A => B, A, B](fn, a, { (bf, ba) => bf.ap(ba) })
      override def product[A, B](a: HyperBuild[M, A], b: HyperBuild[M, B]): HyperBuild[M, (A, B)] =
        Map2[M, A, B, (A, B)](a, b, { (bf, ba) => bf.product(ba) })
    }

  private case class Map2[M[_], A, B, C](
      a: HyperBuild[M, A],
      b: HyperBuild[M, B],
      fn: (Build[M, A], Build[M, B]) => Build[M, C]) extends HyperBuild[M, C] {
    def at(t: Timestamp) = fn(a.at(t), b.at(t))
  }
}
