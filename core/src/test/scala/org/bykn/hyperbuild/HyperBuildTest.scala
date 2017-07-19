package org.bykn.hyperbuild

import cats.{Functor, Monad}
import cats.implicits._
import cats.effect.IO
import org.scalatest.FunSuite

class HyperBuildTest extends FunSuite {

  def assertBuild[T](memo: Memo[IO], hb: HyperBuild[IO, T], at: Timestamp, res: T) = {
    assert(hb.at(at, memo)._2.unsafeRunSync._1 === res)
  }
  def assertIO[T](io: IO[T], t: T) =
    assert(io.unsafeRunSync == t)

  import BasicHyper._

  test("test a basic hyperbuild") {
    // put the timestamp in an IORef

    def makeRefs(n: Int): IO[Vector[IORef[List[Timestamp]]]] =
      (0 until n).toVector.traverse(_ => IORef[List[Timestamp]])

    val buckets = 7

    val memo = new MemoryMemo
    val io = makeRefs(buckets).flatMap { register =>
      val action = Build.mod[IO].unsafeFn { ts: Timestamp =>
        val reg = register(ts.secondsSinceEpoch % buckets)
        for {
          optl <- reg.get
          list = ts :: optl.getOrElse(Nil)
          _ <- reg.set(Some(list))
        } yield ()
      }

      val bld = hourlyAfter[IO, Unit](Timestamp(100))(action)

      for {
        _ <- bld.valueAt(Timestamp(200), memo)
        str0 <- memo.stores
        mis0 <- memo.misses
        _ <- bld.valueAt(Timestamp(200), memo)
        str1 <- memo.stores
        mis1 <- memo.misses
      } yield (str0, mis0, str1, mis1)
    }

    val (s0, m0, s1, m1) = io.unsafeRunSync
    assert(((s0, m0)) == ((s1, m1))) // TODO actually cache here, both are 0
  }
}

object BasicHyper {
  sealed trait IORef[A] {
    def get: IO[Option[A]]
    def set(a: Option[A]): IO[Unit]
  }
  object IORef {
    private[this] class Box[A] extends IORef[A] {
      var value: Option[A] = None
      def get = IO(value)
      def set(a: Option[A]) = IO {
        value = a
        ()
      }
    }
    def apply[A]: IO[IORef[A]] = IO(new Box[A])
  }

  def hourlyAfter[M[_]: Monad, T](init: Timestamp)(ts: Build[M, Timestamp => M[T]]): HyperBuild[M, Option[T]] = {
    val evs = Event.empty.cutover(init, Event.hourly)

    case class MapFn(f: Functor[M]) extends Function1[M[T], M[Option[T]]] with Serializable {
      def apply(m: M[T]): M[Option[T]] = f.map(m)(Some(_))
    }

    val liftToOpt: Build[M, Timestamp => M[Option[T]]] =
      ts.andThen(Build.mod[M].fn[M[T], M[Option[T]], MapFn](MapFn(implicitly[Functor[M]])))
    HyperBuild.eventedM[M, Option[T]](evs, Build.mod[M].noneM[T])(liftToOpt)
  }
}
