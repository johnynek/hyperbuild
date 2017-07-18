package org.bykn.hyperbuild

import cats.implicits._
import org.scalatest.FunSuite
import scala.spores._

class HyperBuildTest extends FunSuite {
  test("test a basic hyperbuild") {

  }
}

object BasicHyper {

  def hourlyAfter[M[_], T](init: Timestamp)(ts: Build[M, Timestamp => T]): HyperBuild[M, Option[T]] = {
    val evs = Event.empty.cutover(init, Event.hourly)

    HyperBuild.evented[M, Option[T]](evs, Build.mod[M].None)(ts.andThen(Build.mod[M].fn(spore { (t: T) => Some(t) })))
  }
}
