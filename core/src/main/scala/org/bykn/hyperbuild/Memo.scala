package org.bykn.hyperbuild

import cats._
import cats.implicits._

trait Memo[M[_]] {
  implicit def monadError: MonadError[M, Throwable]

  // returns the number of keys actually removed
  def remove(key: Fingerprint): M[Int]
  def fetch[T](key: Fingerprint, ser: Serialization[T]): M[Option[T]]
  def store[T](key: Fingerprint, value: M[T], ser: Serialization[T]): M[T]

  def runNamed[T](name: String)(result: => M[T]): M[T]

  final def getOrElseUpdate[T](key: Fingerprint, ser: Serialization[T])(value: => M[T]): M[T] =
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
