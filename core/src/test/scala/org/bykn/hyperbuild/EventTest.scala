package org.bykn.hyperbuild

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

object EventGen {
  val timestampGen: Gen[Timestamp] = Gen.choose(0, 1400000000).map(Timestamp(_))
  implicit val timestampArb: Arbitrary[Timestamp] = Arbitrary(timestampGen)

  def eventGen(depth: Int): Gen[Event] = {
    val zero = List(
      (5, Gen.const(Event.empty)),
      (5, Gen.choose(1, 60 * 60 * 24 * 7).map(Event.everySeconds(_))))

    if (depth <= 0) Gen.frequency(zero: _*)
    else {
      val rec = Gen.lzy(eventGen(depth - 1))

      val cut: Gen[Event] =
        for {
          t <- timestampGen
          a <- rec
          b <- rec
        } yield a.cutover(t, b)

      val merge: Gen[Event] =
        Gen.lzy(for {
          a <- rec
          b <- rec
        } yield a.merge(b))

      val lookback: Gen[Event] =
        for {
          delta <- Gen.choose(0, 60 * 60 * 24 * 7)
          e <- rec
        } yield e.shift(delta)

      Gen.frequency(
        zero ::: (
        (1, cut) ::
        (1, merge) ::
        (1, lookback) :: Nil): _*)
    }
  }

  implicit val arbEvent: Arbitrary[Event] =
    Arbitrary(Gen.choose(0, 200).flatMap(eventGen(_)))
}

class EventTest extends FunSuite {

  import EventGen._

  def bounds(e: Either[(Timestamp, Timestamp), Timestamp]): (Option[Timestamp], Timestamp) =
    e match {
      case Right(u) => (None, u)
      case Left((a, b)) =>
        val l = Ordering[Timestamp].min(a, b)
        val u = Ordering[Timestamp].max(a, b)
        (Some(l), u)
    }

  test("identity merge") {
    forAll { (a: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val left = a merge Event.empty
      val right = Event.empty merge a
      val (lower, upper) = bounds(e)
      assert(left.mostRecent(lower, upper) === a.mostRecent(lower, upper))
      assert(right.mostRecent(lower, upper) === a.mostRecent(lower, upper))
    }
  }

  test("commutative merge") {
    forAll { (a: Event, b: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val left = a merge b
      val right = b merge a
      val (lower, upper) = bounds(e)
      assert(left.mostRecent(lower, upper) === right.mostRecent(lower, upper))
    }
  }

  test("merge is like max") {
    forAll { (a: Event, b: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val (lower, upper) = bounds(e)
      val ar = a.mostRecent(lower, upper)
      val br = b.mostRecent(lower, upper)
      val cr = (a merge b).mostRecent(lower, upper)
      val ord = Ordering[Option[Timestamp]]
      if (ord.gt(ar._1, br._1)) {
        assert(cr._1 === ar._1)
      }
      else if (ord.lt(ar._1, br._1)) {
        assert(cr._1 === br._1)
      }
      else {
        assert(cr._1 === ar._1)
      }
    }
  }

  test("associative merge") {
    forAll { (a: Event, b: Event, c: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val left = (a merge b) merge c
      val right = a merge (b merge c)
      val (lower, upper) = bounds(e)
      assert(left.mostRecent(lower, upper) === right.mostRecent(lower, upper))
    }
  }

  test("idempotent merge") {
    forAll { (a: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val res = a merge a
      val (lower, upper) = bounds(e)
      assert(res.mostRecent(lower, upper) === a.mostRecent(lower, upper))
    }
  }

  test("if two hashes are the same, all interior points are the same") {
    forAll { (a: Event, b: Event, pt1: Timestamp, pt2: Timestamp, rest: List[Timestamp]) =>
      val allPoints = (pt1 :: pt2 :: rest).sorted

      val lower = allPoints.head
      val upper = allPoints.last

      val middle = allPoints.drop(1).dropRight(1)

      val ares = a.mostRecent(Some(lower), upper)
      val bres = b.mostRecent(Some(lower), upper)

      if (ares === bres) {
        // if you have the same result, including hash, all interior points
        // are the same:
        middle.foreach { ts =>
          assert(a.mostRecent(Some(lower), ts) === b.mostRecent(Some(lower), ts))
        }
      }
    }
  }

  test("shift is a homomorphism") {
    forAll { (a: Event, delta1: Short, delta2: Short, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val (l, u) = bounds(e)
      // avoid complex overflow issues
      val d1 = delta1.toInt
      val d2 = delta2.toInt
      assert(a.shift(d1.toInt).shift(d2.toInt).mostRecent(l, u) === a.shift(d1 + d2).mostRecent(l, u))
    }
  }

  test("cutover doesn't change results before") {
    forAll { (a: Event, b: Event, at: Timestamp, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val (l, u) = bounds(e)
      val c = a.cutover(at, b)

      if (Ordering[Timestamp].gteq(at, u)) {
        // Same as a *INCLUDING* the Fingerprint
        assert(a.mostRecent(l, u) === c.mostRecent(l, u))
      }
      else {
        // at < u, maybe
        // the timestamp should be the same as b, but maybe not the hash
        b.mostRecent(Some(at), u)._1 match {
          case None =>
            // if b has nothing, we fall back to a
            assert(a.mostRecent(l, at)._1 === c.mostRecent(l, u)._1)
          case nonEmpty =>
            assert(nonEmpty === c.mostRecent(l, u)._1)
        }
      }
    }
  }

  test("periodic + shift emits times divisable by the period + shift") {
    forAll { (i: Int, shift0: Int, ts: Timestamp) =>
      val pos0 = i & Int.MaxValue
      val pos = if (pos0 == 0) 1 else pos0

      val shift = shift0 & ((1 << 20) - 1) // don't make giant shifts with overflow issues
      val ev = Event.everySeconds(pos).shift(shift)

      def mod(x: Int): Int = {
        val rem = x % pos
        if (rem < 0) rem + pos
        else rem
      }
      ev.mostRecent(None, ts)._1 match {
        case Some(resStamp) =>
          assert(mod(resStamp.secondsSinceEpoch) === mod(shift))
        case None =>
          assert(ts.secondsSinceEpoch == Int.MinValue)
      }
    }
  }

  test("most recent emits timestamps in range") {
    forAll { (a: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val (l, u) = bounds(e)
      a.mostRecent(l, u)._1 match {
        case None =>
          // can't say anything here
          ()
        case Some(ts) =>
          l match {
            case Some(lower) => assert(Ordering[Timestamp].lteq(lower, ts))
            case None => ()
          }
          assert(Ordering[Timestamp].lt(ts, u))
      }
    }
  }

  test("hash does not change just after the most recent event") {
    forAll { (a: Event, e: Either[(Timestamp, Timestamp), Timestamp]) =>
      val (l, u) = bounds(e)

      a.mostRecent(l, u) match {
        case res@(Some(ts), fp) =>
          assert(a.mostRecent(l, Timestamp(ts.secondsSinceEpoch + 1)) === res)
        case (None, _) => ()
      }
    }
  }

  test("some specific cases") {
    {
      val p1 = Event.everySeconds(24989)
      val ts = Timestamp(233658286)
      assert(p1.mostRecent(None, ts) === (p1 merge p1).mostRecent(None, ts))
    }

    {
      val p2 = Event.everySeconds(167898)
      val at = Timestamp(340959556)
      val cutover = p2.cutover(at, Event.empty)
      cutover.mostRecent(Some(Timestamp(616382162)), Timestamp(722931314))._1 match {
        case Some(ts) =>
          assert(Ordering[Timestamp].lteq(Timestamp(616382162), ts), s"$ts out of range")
        case None =>
          ()
      }
    }

    {
      // Shift homomorphism
      val s1 = 1748032458
      val s2 = -2321
      val p = Event.everySeconds(139108)
      val ev1 = p.shift(s1).shift(s2)
      val ev2 = p.shift(s1 + s2)
      assert(ev1.mostRecent(Some(Timestamp(82969244)), Timestamp(1054847326)) === ev2.mostRecent(Some(Timestamp(82969244)), Timestamp(1054847326)),
        s"$ev1 != $ev2")
    }

    {
      // Shift homomorphism
      val s1 = 1
      val s2 = -1
      val p = Event.everySeconds(6804)
      val ev1 = p.shift(s1).shift(s2)
      val ev2 = p.shift(s1 + s2)
      assert(ev1.mostRecent(Some(Timestamp(209747005)), Timestamp(347844728)) === ev2.mostRecent(Some(Timestamp(209747005)), Timestamp(347844728)),
        s"$ev1 != $ev2")
    }
  }
}
