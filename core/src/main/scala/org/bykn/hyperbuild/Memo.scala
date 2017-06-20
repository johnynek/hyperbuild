package org.bykn.hyperbuild

import cats._
import cats.implicits._

trait Memo[M[_]] {
  implicit def monadError: MonadError[M, Throwable]

  def fetch[T](key: Fingerprint, ser: Serialization[T]): M[Option[(T, Fingerprint)]]
  def store[T](key: Fingerprint, value: M[T], ser: Serialization[T]): M[(T, Fingerprint)]

  def runNamed[T](name: String)(result: => M[T]): M[T]

  final def getOrElseUpdate[T](key: Fingerprint, ser: Serialization[T])(value: => M[T]): M[(T, Fingerprint)] =
    fetch(key, ser).flatMap {
      case None =>
        for {
          mt <- monadError.catchNonFatal(value)
          t <- store(key, mt, ser)
        } yield t
      case Some(t) =>
        monadError.pure(t)
    }
}
