package org.bykn.hyperbuild

import org.scalatest.FunSuite
import cats.effect.IO
//import cats.implicits._

import scala.spores._

object Mutable extends java.io.Serializable {
  var evilBuilds: Int = 0
}
/**
 * spores can't be inside FunSuite, they throw at runtime (so much
 * for compile time safety)
 */
object Examples {
  val forty2 = Build.pure[IO, Int](42)
    .cached
    .mapCached(spore { _ * 42 })

  val evil = Build.pure[IO, Int](42)
    .cached
    .mapCached(spore {
      val ex = Mutable
      i => ex.evilBuilds += 1; i * 42
    })
}

class BuildTest extends FunSuite {
  def assertBuild[A](m: Memo[IO], b: Build[IO, A], a: A) =
    assertIO(b.run(m).map(_._1), a)

  def assertIO[T](io: IO[T], t: T) =
    assert(io.unsafeRunSync == t)

  test("builds work") {
    val memo = new MemoryMemo
    assertBuild(memo, Examples.forty2, 42 * 42)
  }

  test("cached builds work") {
    val memo = new MemoryMemo

    assertBuild(memo, Examples.forty2, 42 * 42)
    assertIO(memo.stores, 1L)
    assertIO(memo.hits, 0L)
    assertIO(memo.misses, 1L)

    // when we run again, we hit
    assertBuild(memo, Examples.forty2, 42 * 42)
    assertIO(memo.stores, 1L)
    assertIO(memo.hits, 1L)
    assertIO(memo.misses, 1L)

  }

  test("mutation changes function, invalidating caches") {
    val memo = new MemoryMemo

    Mutable.evilBuilds = 0
    assertBuild(memo, Examples.evil, 42 * 42)
    assert(Mutable.evilBuilds == 1)
    assertIO(memo.stores, 1L)
    assertIO(memo.hits, 0L)
    assertIO(memo.misses, 1L)

    assertBuild(memo, Examples.evil, 42 * 42)
    assert(Mutable.evilBuilds == 2)
    assertIO(memo.stores, 2L)
    assertIO(memo.hits, 0L)
    assertIO(memo.misses, 2L)
  }
}
