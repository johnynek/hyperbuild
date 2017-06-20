package org.bykn.hyperbuild

import cats.{FunctorFilter, Semigroup, Monoid}

sealed trait Event[T] {
  import Event.{Cutover, OptMapped, Merge, LookBack}
  // return the most recent value before the given Timestamp
  def mostRecent(t: Timestamp): Option[(Timestamp, T)]

  def withTimestampOptionMap[U](fn: (Timestamp, T) => Option[U]): Event[U] =
    OptMapped[T, U](this, fn)

  def merge(that: Event[T])(implicit semi: Semigroup[T]): Event[T] =
    Merge(this, that, semi)

  def withTimestamp: Event[(Timestamp, T)] =
    withTimestampOptionMap { (ts, t) => Some((ts, t)) }

  /**
   * At and after a given timestamp, cutover to a new
   * event stream
   */
  def cutover(at: Timestamp, to: Event[T]): Event[T] =
    Cutover(at, this, to)

  def optionMap[U](fn: T => Option[U]): Event[U] =
    withTimestampOptionMap[U]((_, t) => fn(t))

  def map[U](fn: T => U): Event[U] =
    optionMap(fn.andThen(Some(_)))

  def filter(fn: T => Boolean): Event[T] =
    optionMap { t => if (fn(t)) Some(t) else None }

  /**
   * look back a number of seconds
   */
  def lookBack(seconds: Int): Event[T] =
    LookBack(this, seconds)
}

object Event {

  def empty[A]: Event[A] = Empty[A]()
  def everySeconds(period: Int): Event[Timestamp] =
    Periodic(period).withTimestampOptionMap { (ts, _) => Some(ts) }

  val hourly: Event[Timestamp] = everySeconds(60 * 60)
  val daily: Event[Timestamp] = everySeconds(60 * 60 * 24)

  private case class Empty[A]() extends Event[A] {
    def mostRecent(t: Timestamp) = None
  }

  private case class Merge[A](left: Event[A], right: Event[A], semigroup: Semigroup[A]) extends Event[A] {
    def mostRecent(t: Timestamp) =
      (left.mostRecent(t), right.mostRecent(t)) match {
        case (None, right) => right
        case (left, None) => left
        case (lsome@Some((leftTs, leftA)), rsome@Some((rightTs, rightA))) =>
          val cmp = leftTs.secondsSinceEpoch.compareTo(rightTs.secondsSinceEpoch)
          if (cmp == 0) Some((leftTs, semigroup.combine(leftA, rightA)))
          else if (cmp > 0) lsome // left is most recent
          else rsome
      }
  }

  private case class OptMapped[T, U](ev: Event[T], fn: (Timestamp, T) => Option[U]) extends Event[U] {
    def mostRecent(t: Timestamp) = {
      @annotation.tailrec
      def go(ts: Timestamp): Option[(Timestamp, U)] =
        ev.mostRecent(ts) match {
          case None        => None // explicit flatMap to get tailRec
          case Some((at, t)) =>
            fn(at, t) match {
              case Some(u) => Some((at, u))
              case None    => go(at) // back track more
            }
        }

      go(t)
    }
  }

  private case class Cutover[T](at: Timestamp, before: Event[T], onAfter: Event[T]) extends Event[T] {
    def mostRecent(t: Timestamp): Option[(Timestamp, T)] =
      if (t.secondsSinceEpoch <= at.secondsSinceEpoch) before.mostRecent(t)
      else {
        onAfter.mostRecent(t) match {
          case s@Some((ts, _)) if ts.secondsSinceEpoch >= at.secondsSinceEpoch => s
          case _ =>
            // maybe there is something before:
            before.mostRecent(at)
        }
    }
  }

  private case class Periodic(period: Int) extends Event[Unit] {
    def mostRecent(t: Timestamp): Option[(Timestamp, Unit)] = {
      val recentTs = (t.secondsSinceEpoch / period) * period
      val resTs =
        if (recentTs == t.secondsSinceEpoch) Timestamp(recentTs - period)
        else Timestamp(recentTs)

      Some((resTs, ()))
    }
  }

  private case class LookBack[T](of: Event[T], delta: Int) extends Event[T] {
    def mostRecent(t: Timestamp) =
      of.mostRecent(t.delta(-delta))
        .map { case (ts, t) => (ts.delta(delta), t) }
  }

  implicit def eventFunctorFilter: FunctorFilter[Event] = new FunctorFilter[Event] {
    def mapFilter[A, B](ev: Event[A])(fn: A => Option[B]) = ev.optionMap(fn)
    def map[A, B](ev: Event[A])(fn: A => B) = ev.map(fn)
  }

  implicit def eventMonoid[A: Semigroup]: Monoid[Event[A]] = new Monoid[Event[A]] {
    def empty = Empty[A]()
    def combine(l: Event[A], r: Event[A]) = l.merge(r)
  }
}

