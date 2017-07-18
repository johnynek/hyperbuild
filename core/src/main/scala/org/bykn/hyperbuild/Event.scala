package org.bykn.hyperbuild

import cats.Monoid

sealed trait Event {
  import Event.{Cutover, Merge, LookBack}
  /**
   * return the most recent value strictly before the exUpper and
   * optionally greater than or equal to lower. The fingerprint
   * should only cover the included interval
   */
  def mostRecent(lower: Option[Timestamp], exUpper: Timestamp): (Option[Timestamp], Fingerprint)

  def merge(that: Event): Event =
    Merge(this, that)

  /**
   * At and after a given timestamp, cutover to a new
   * event stream
   */
  def cutover(at: Timestamp, to: Event): Event =
    Cutover(at, this, to)

  /**
   * look back a number of seconds
   */
  def lookBack(seconds: Int): Event =
    LookBack(this, seconds)
}

object Event {

  def empty: Event = Empty
  def everySeconds(period: Int): Event =
    Periodic(period)

  val hourly: Event = everySeconds(60 * 60)
  val daily: Event = everySeconds(60 * 60 * 24)

  private case object Empty extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) =
      (None, Fingerprint.combineAll(Fingerprint("Event.empty") :: Nil))
  }

  private case class Merge(left: Event, right: Event) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      val (optl, l) = left.mostRecent(lower, exUpper)
      val (optr, r) = right.mostRecent(lower, exUpper)
      def newFP = Fingerprint.combineAll((l :: r :: Nil).sortBy(_.toS))
      (optl, optr) match {
        case (None, right) => (right, newFP)
        case (left, None) => (left, newFP)
        case (lsome@Some(leftTs), rsome@Some(rightTs)) =>
          val cmp = leftTs.secondsSinceEpoch.compareTo(rightTs.secondsSinceEpoch)
          if (cmp >= 0) (lsome, newFP)
          else (rsome, newFP)
      }
    }
  }

  private case class Cutover(at: Timestamp, before: Event, onAfter: Event) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      if (exUpper.secondsSinceEpoch < at.secondsSinceEpoch) before.mostRecent(lower, exUpper)
      else {
        val bef@(optTsB, fpB) = before.mostRecent(lower, at)
        val aft@(optTsA, fpA) = onAfter.mostRecent(Some(at), exUpper)
        optTsA match {
          case None =>
            // there is actually no event in the onAfter region
            bef
          case some =>
            // need to combine the fingerprints
            (some, Fingerprint.combine(fpB, fpA))
        }
      }
    }
  }

  private case class Periodic(period: Int) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      def floor(t: Timestamp): Int = (t.secondsSinceEpoch / period) * period

      val recentTs = floor(exUpper)
      val optLower = lower.map(floor)
      val resTs =
        if (floor(exUpper) == exUpper.secondsSinceEpoch) Timestamp(recentTs - period)
        else Timestamp(recentTs)

      val cycles = (exUpper.secondsSinceEpoch / period) - (lower.fold(Int.MinValue)(_.secondsSinceEpoch) / period)
      (Some(resTs), Fingerprint.combine(Fingerprint(s"Event.Periodic($period)"), Fingerprint(cycles.toString)))
    }
  }

  private case class LookBack(of: Event, delta: Int) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      val (opt, fp) = of.mostRecent(lower.map(_.delta(-delta)), exUpper.delta(-delta))
      (opt.map(_.delta(delta)), Fingerprint.combine(Fingerprint(s"Event.Lookback(_, $delta)"), fp))
    }
  }

  implicit val eventMonoid: Monoid[Event] = new Monoid[Event] {
    def empty = Empty
    def combine(l: Event, r: Event) = l.merge(r)
  }
}

