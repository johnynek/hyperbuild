package org.bykn.hyperbuild

import cats.Semigroup

case class Timestamp(secondsSinceEpoch: Int) {
  def delta(t: Int): Timestamp = Timestamp(secondsSinceEpoch + t)
}

object Timestamp {
  implicit val timestampOrdering: Ordering[Timestamp] =
    Ordering.by(_.secondsSinceEpoch)

  implicit val timestampSemigroup: Semigroup[Timestamp] =
    new Semigroup[Timestamp] {
      def combine(a: Timestamp, b: Timestamp) = Timestamp(a.secondsSinceEpoch max b.secondsSinceEpoch)
    }

  implicit def hasFingerprint[M[_]]: HasFingerprint[M, Timestamp] =
    HasFingerprint.from[M, Timestamp] { case Timestamp(s) => Fingerprint.combine(Fingerprint("Timestamp"), Fingerprint(s.toString)) }
}
