package org.bykn.hyperbuild

import cats._
import cats.implicits._

trait Memo[M[_]] {
  implicit def monadError: MonadError[M, Throwable]

  def fetch[T](key: String, inputs: Fingerprint, ser: Serialization[T]): M[Option[(T, Fingerprint)]]
  def store[T](key: String, inputs: Fingerprint, value: M[T], ser: Serialization[T]): M[T]

  final def getOrElseUpdate[T](key: String, inputs: Fingerprint, ser: Serialization[T])(value: => M[T]): M[(T, Fingerprint)] =
    fetch(key, inputs, ser).flatMap {
      case None =>
        for {
          mt <- monadError.catchNonFatal(value)
          t <- store(key, inputs, mt, ser)
        } yield (t, Fingerprint.of(t)(ser))
      case Some(t) =>
        monadError.pure(t)
    }
}
