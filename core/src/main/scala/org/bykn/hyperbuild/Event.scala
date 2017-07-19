package org.bykn.hyperbuild

import cats.Monoid

/**
 * Event represents a function to generate a series of
 * timestamps. Given an interval, it can return the most
 * recent timestamp as well as a Fingerprint for all timestamps
 * up to that one in the given interval
 */
sealed trait Event extends Serializable {
  import Event.{Cutover, Empty, Merge, Periodic, Shift}
  /**
   * return the most recent value strictly before the exUpper and
   * optionally greater than or equal to lower. The fingerprint
   * is effectively a hash of all the timestamps included in
   * this interval.
   */
  def mostRecent(lower: Option[Timestamp], exUpper: Timestamp): (Option[Timestamp], Fingerprint)

  def merge(that: Event): Event =
    that match {
      case Empty => this
      case notEmptyThat =>
        this match {
          case Empty => notEmptyThat
          case notEmptyThis => Merge(notEmptyThis, notEmptyThat)
        }
    }

  /**
   * At and after a given timestamp, cutover to a new
   * event stream
   */
  def cutover(at: Timestamp, to: Event): Event =
    Cutover(at, this, to)

  /**
   * shift this Event stream to the right by some number of seconds
   * (which is to say, delay it some number of seconds)
   */
  final def shift(seconds: Int): Event =
    if (seconds == 0) this
    else {
      def mod(x: Int, n: Int): Int = {
        val m0 = x % n
        if (m0 < 0) m0 + n else m0
      }

      this match {
        case Empty => Empty
        case Merge(a, b) =>
          Merge(a.shift(seconds), b.shift(seconds))
        case periodic@Periodic(p) =>
          // no need shift more than the period
          val s1 = mod(seconds, p)
          if (s1 == 0) periodic
          else Shift(periodic, s1)
        case Shift(periodic@Periodic(p), s) =>
          // no need shift more than the period
          val s1 = mod(seconds + s, p)
          if (s1 == 0) periodic
          else Shift(periodic, s1)
        case Shift(of, s) =>
          of.shift(s + seconds)
        case notlb =>
          Shift(notlb, seconds)
      }
    }
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
    private lazy val flatten: Set[Event] = {

      def loop(node: Event, toVisit: List[Event], processed: Set[Event]): Set[Event] = node match {
        case Merge(l, r) if processed(l) => loop(r, toVisit, processed)
        case Merge(l, r) => loop(r, l :: toVisit, processed)
        case other => toVisit match {
          case Nil => processed + other
          case h :: tail => loop(h, tail, processed + other)
        }
      }
      loop(this, Nil, Set.empty)
    }

    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      val allItems = flatten.map(_.mostRecent(lower, exUpper))
      val (maxTime, _) = allItems.maxBy(_._1)
      val collisionSet: Set[Fingerprint] = allItems.collect { case (t, fp) if t == maxTime => fp }
      val collisions = collisionSet.toList.sortBy(_.toS)
      val fp = collisions match {
        case h :: Nil => h
        case nonSingle => Fingerprint.combineAll(nonSingle)
      }
      (maxTime, fp)
    }
  }

  private case class Cutover(at: Timestamp, before: Event, onAfter: Event) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      if (exUpper.secondsSinceEpoch < at.secondsSinceEpoch) before.mostRecent(lower, exUpper)
      else {
        val bef@(optTsB, fpB) = before.mostRecent(lower, at)
        val (optTsA, fpA) = onAfter.mostRecent(Some(at), exUpper)
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

      val lowerFloor = lower.map { l =>
        val lf = floor(l)
        // if the actual lower bound is a period boundary, great, else take next
        if (lf == l) l else lf + period
      }
      val recentTs = floor(exUpper)
      val optLower = lower.map(floor)
      val resTs =
        if (floor(exUpper) == exUpper.secondsSinceEpoch) Timestamp(recentTs - period)
        else Timestamp(recentTs)

      // note resTs < exUpper, but it may not be >= lower
      val optRes = lower match {
        case None => Some(resTs)
        case Some(lb) if Ordering[Timestamp].lteq(lb, resTs) => Some(resTs)
        case _ => None
      }

      (optRes, Fingerprint.combine(Fingerprint("Event.Periodic"), Fingerprint(s"($period, $lowerFloor, $recentTs)")))
    }
  }

  private case class Shift(of: Event, delta: Int) extends Event {
    def mostRecent(lower: Option[Timestamp], exUpper: Timestamp) = {
      val (opt, fp) = of.mostRecent(lower.map(_.delta(-delta)), exUpper.delta(-delta))
      (opt.map(_.delta(delta)), Fingerprint.combine(Fingerprint(s"Event.Shift(_, $delta)"), fp))
    }
  }

  implicit val eventMonoid: Monoid[Event] = new Monoid[Event] {
    def empty = Empty
    def combine(l: Event, r: Event) = l.merge(r)
  }
}

