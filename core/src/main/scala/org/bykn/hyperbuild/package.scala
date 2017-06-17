package org.bykn

import com.twitter.bijection.Injection
import cats.effect.IO

package object hyperbuild {
  type Serialization[T] = Injection[T, Array[Byte]]

  val IOModule: Build.Module[IO] = Build.mod[IO]
}

