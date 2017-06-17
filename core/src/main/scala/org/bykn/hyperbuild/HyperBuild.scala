package org.bykn.hyperbuild

import cats.implicits._

sealed trait HyperBuild[M[_], T] {
  def at(t: Timestamp): Build[M, T]
}

object HyperBuild {

  def build[M[_], T, U](ev: Event[T], init: Build[M, U])(fn: T => Build[M, U]): HyperBuild[M, U] =
    Evented(ev, fn, init)

  case class Evented[M[_], T, U](ev: Event[T], fn: T => Build[M, U], init: Build[M, U]) extends HyperBuild[M, U] {
    def at(t: Timestamp) = ev.mostRecent(t) match {
      case Some((_, t)) => fn(t)
      case None => init
    }
  }
  case class Zip[M[_], A, B](a: HyperBuild[M, A], b: HyperBuild[M, B]) extends HyperBuild[M, (A, B)] {
    def at(t: Timestamp) = a.at(t).product(b.at(t))
  }
  case class Map[M[_], A, B](a: HyperBuild[M, A], fn: A => B) extends HyperBuild[M, B] {
    def at(t: Timestamp) = a.at(t).map(fn)
  }
}
