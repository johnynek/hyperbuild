package org.bykn.hyperbuild

import cats.implicits._
import cats.effect.{ IO, Sync }
import java.util.concurrent.atomic.AtomicLong

class MemoryMemo extends Memo[IO] {
  private[this] val cache: collection.mutable.Map[Fingerprint, Array[Byte]] = collection.mutable.Map()

  private def onCache[T](fn: collection.mutable.Map[Fingerprint, Array[Byte]] => T): T = cache.synchronized {
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

  def runNamed[T](name: String)(b: => IO[T]): IO[T] =
    for {
      _ <- IO(println(s"building: $name"))
      start <- IO(System.nanoTime)
      t <- b
      end <- IO(System.nanoTime)
      _ <- IO(System.out.printf(s"done: $name in %.2fms\n", java.lang.Double.valueOf((end - start)/1000000.0)))
    } yield t

  def fetch[T](key: Fingerprint, ser: Serialization[T]): IO[Option[T]] = IO {
    onCache(_.get(key)) match {
      case None =>
        val m = missesA.incrementAndGet
        log(s"cache miss: $key, $m") >> IO.pure(None)
      case Some(b) =>
        val h = hitsA.incrementAndGet
        log(s"cache hit: $key, $h")
        monadError.fromTry(ser.invert(b)).map { t => Some(t) }
    }
  }.flatten

  def store[T](key: Fingerprint, value: IO[T], ser: Serialization[T]): IO[T] = value.flatMap { t =>
    IO {
      val s = storesA.incrementAndGet
      val bytes = ser(t)
      onCache(_.update(key, bytes))
      log(s"store: $key, $s").map(_ => t)
    }.flatten
  }
}
