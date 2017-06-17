package org.bykn.hyperbuild

import java.security.MessageDigest

/**
 * the Fingerprint is a signature of content.
 * It is a hash of everything that went in
 * to producing the output
 */
case class Fingerprint(toS: String)
object Fingerprint {
  private def hash(fn: MessageDigest => Unit): Fingerprint = {
    val hash = MessageDigest.getInstance("SHA-256")
    fn(hash)
    Fingerprint(String.format("%064x", new java.math.BigInteger(1, hash.digest)))
  }

  def combine(a: Fingerprint, b: Fingerprint): Fingerprint = hash { fn =>
    fn.update(a.toS.getBytes)
    fn.update(b.toS.getBytes)
  }

  def of[A](a: A)(implicit ser: Serialization[A]): Fingerprint = hash { fn =>
    fn.update(ser(a))
  }
}
