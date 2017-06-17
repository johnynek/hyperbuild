package org.bykn

import com.twitter.bijection.Injection

package object hyperbuild {
  type Serialization[T] = Injection[T, Array[Byte]]
}

