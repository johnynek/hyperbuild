package org.bykn.hyperbuild

import com.twitter.bijection.Injection
import cats.Monad
import cats.implicits._
import cats.effect.IO
import org.scalatest.FunSuite
import scala.util.Try

class HyperBuildTest extends FunSuite {

  def assertBuild[T](memo: Memo[IO], hb: HyperBuild[IO, T], at: Timestamp, res: T) = {
    assert(hb.valueAt(at, memo).unsafeRunSync === res)
  }
  def assertIO[T](io: IO[T], t: T) =
    assert(io.unsafeRunSync == t)

  import BasicHyper._

  test("test a basic hyperbuild") {
    // put the timestamp in an IORef

    def makeRefs(n: Int): IO[Vector[IORef[List[Timestamp]]]] =
      (0 until n).toVector.traverse(_ => IORef[List[Timestamp]])

    val buckets = 7

    val io = makeRefs(buckets).flatMap { register =>
      val action = Build.mod[IO].constWithFingerprint({ ts: Timestamp =>
        val reg = register(ts.secondsSinceEpoch % buckets)
        for {
          optl <- reg.get
          list = ts :: optl.getOrElse(Nil)
          _ <- reg.set(Some(list))
        } yield ()
      }, Fingerprint("set register"))

      implicit val serU: Serialization[Unit] =
        new Injection[Unit, Array[Byte]] {
          val empty = new Array[Byte](0)
          val success = Try(())
          def apply(u: Unit) = empty
          def invert(b: Array[Byte]): Try[Unit] =
            if (b.length == 0) success
            else Try(sys.error(s"found len: ${b.length}"))
        }

      val bld = hourlyAfter[IO](Timestamp(100))(action).cached

      val memo = new MemoryMemo

      for {
        _ <- bld.valueAt(Timestamp(200), memo)
        str0 <- memo.stores
        h0 <- memo.hits
        mis0 <- memo.misses
        _ <- bld.valueAt(Timestamp(200), memo)
        str1 <- memo.stores
        h1 <- memo.hits
        mis1 <- memo.misses
      } yield (str0, h0, mis0, str1, h1, mis1)
    }

    val (s0, h0, m0, s1, h1, m1) = io.unsafeRunSync
    assert(((s0, m0)) == ((1, 1)))
    assert(((s0, m0)) == ((s1, m1)))
    assert(h0 === 0)
    assert(h1 === 1)
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

  def hourlyAfter[M[_]: Monad](init: Timestamp)(ts: Build[M, Timestamp => M[Unit]]): HyperBuild[M, Unit] = {
    val evs = Event.empty.cutover(init, Event.hourly)

    HyperBuild.evented[M, M[Unit]](evs, Build.mod[M].unit.pureBuild)(ts)
      .flatten
  }
}
