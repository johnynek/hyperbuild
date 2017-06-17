package org.bykn.hyperbuild

sealed trait Event[T] {
  // return the most recent value before the given Timestamp
  def mostRecent(t: Timestamp): Option[(Timestamp, T)]
}

object Event {
  case class Periodic(offset: Timestamp, period: Int) extends Event[Timestamp] {
    def mostRecent(t: Timestamp): Option[(Timestamp, Timestamp)] = ???
  }
  case class Filter[T](ev: Event[T], fn: T => Boolean) extends Event[T] {
    def mostRecent(t: Timestamp) = {
      @annotation.tailrec
      def go(ts: Timestamp): Option[(Timestamp, T)] =
        ev.mostRecent(t) match {
          case s@Some((_, t)) if fn(t) => s
          case Some((newTs, _)) => go(newTs)
          case None => None
        }

      go(t)
    }
  }
}

