package org.bykn.hyperbuild

import cats.implicits._
import cats.effect.{ IO, Sync }
import java.util.concurrent.atomic.AtomicLong

class MemoryMemo extends Memo[IO] {
  private[this] val cache: collection.mutable.Map[(String, Fingerprint), (Array[Byte], Fingerprint)] = collection.mutable.Map()

  private def onCache[T](fn: collection.mutable.Map[(String, Fingerprint), (Array[Byte], Fingerprint)] => T): T = cache.synchronized {
    fn(cache)
  }

  private[this] val hitsA: AtomicLong = new AtomicLong
  private[this] val missesA: AtomicLong = new AtomicLong
  private[this] val storesA: AtomicLong = new AtomicLong

  def hits: IO[Long] = IO(hitsA.get)
  def misses: IO[Long] = IO(missesA.get)
  def stores: IO[Long] = IO(storesA.get)

  protected def log(msg: => String): IO[Unit] =
    IO.pure(())
    //IO(println(msg))

  def monadError = implicitly[Sync[IO]]

  def fetch[T](key: String, inputs: Fingerprint, ser: Serialization[T]): IO[Option[(T, Fingerprint)]] = IO {
    onCache(_.get((key, inputs))) match {
      case None =>
        val m = missesA.incrementAndGet
        log(s"cache miss: $key, $inputs, $m") >> IO.pure(None)
      case Some((b, fp)) =>
        val h = hitsA.incrementAndGet
        log(s"cache hit: $key, $inputs, $h")
        monadError.fromTry(ser.invert(b)).map { t => Some((t, fp)) }
    }
  }.flatten

  def store[T](key: String, inputs: Fingerprint, value: IO[T], ser: Serialization[T]): IO[T] = value.flatMap { t =>
    IO {
      val s = storesA.incrementAndGet
      onCache(_.update((key, inputs), (ser(t), Fingerprint.of(t)(ser))))
      log(s"store: $key, $inputs, $s").map(_ => t)
    }.flatten
  }
}
