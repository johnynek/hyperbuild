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
        } yield e.lookBack(delta)

      Gen.frequency(
        zero ::: (
        (1, cut) ::
        (1, merge) ::
        (1, lookback) :: Nil): _*)
    }
  }

  implicit val arbEvent: Arbitrary[Event] = Arbitrary(eventGen(8))
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

  test("some specific cases") {
    val p1 = Event.everySeconds(24989)
    val ts = Timestamp(233658286)
    assert(p1.mostRecent(None, ts) === (p1 merge p1).mostRecent(None, ts))
  }
}
