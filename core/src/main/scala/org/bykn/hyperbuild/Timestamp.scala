package org.bykn.hyperbuild

import cats.Semigroup

case class Timestamp(secondsSinceEpoch: Int) extends Ordered[Timestamp] {
  def delta(t: Int): Timestamp = Timestamp(secondsSinceEpoch + t)

  def compare(that: Timestamp) =
    Integer.compare(secondsSinceEpoch, that.secondsSinceEpoch)
}

object Timestamp {
  implicit val timestampOrdering: Ordering[Timestamp] =
    Ordering.by(_.secondsSinceEpoch)

  implicit val timestampSemigroup: Semigroup[Timestamp] =
    new Semigroup[Timestamp] {
      def combine(a: Timestamp, b: Timestamp) = Timestamp(a.secondsSinceEpoch max b.secondsSinceEpoch)
    }

  def fingerprint(ts: Timestamp): Fingerprint =
    Fingerprint.combine(Fingerprint("Timestamp"), Fingerprint(ts.secondsSinceEpoch.toString))

  implicit def hasFingerprint[M[_]]: HasFingerprint[M, Timestamp] =
    HasFingerprint.from[M, Timestamp](fingerprint(_))

  implicit def optionTimestampHasFingerprint[M[_]]: HasFingerprint[M, Option[Timestamp]] =
    HasFingerprint.from[M, Option[Timestamp]] {
      case None => Fingerprint.ofString("scala.None")
      case Some(a) => Fingerprint.combine(Fingerprint("scala.Some"), fingerprint(a))
    }
}
