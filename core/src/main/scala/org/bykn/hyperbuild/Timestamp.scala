package org.bykn.hyperbuild

case class Timestamp(secondsSinceEpoch: Int) {
  def delta(t: Int): Timestamp = Timestamp(secondsSinceEpoch + t)
}
