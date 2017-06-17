package org.bykn.hyperbuild

import java.security.MessageDigest
import java.io.{File, FileInputStream}
import cats.MonadError

/**
 * the Fingerprint is a signature of content.
 * It is a hash of everything that went in
 * to producing the output
 */
case class Fingerprint(toS: String) {
  def bytes: Array[Byte] = toS.getBytes("utf-8")
}

object Fingerprint {
  private def hash(fn: MessageDigest => Unit): Fingerprint = {
    val hash = MessageDigest.getInstance("SHA-256")
    fn(hash)
    Fingerprint(String.format("%064x", new java.math.BigInteger(1, hash.digest)))
  }

  def combine(a: Fingerprint, b: Fingerprint): Fingerprint = hash { fn =>
    fn.update(a.bytes)
    fn.update(b.bytes)
  }

  def combineAll(as: List[Fingerprint]): Fingerprint = hash { fn =>
    as.foreach(f => fn.update(f.bytes))
  }

  def of[A](a: A)(implicit ser: Serialization[A]): Fingerprint =
    hash(_.update(ser(a)))

  def ofBytes(bs: Array[Byte]): Fingerprint = hash(_.update(bs))

  def fromFile[M[_]](f: File)(implicit M: MonadError[M, Throwable]): M[Fingerprint] =
    M.catchNonFatal(hash { fn =>
      val fis = new FileInputStream(f)
      val buffer = new Array[Byte](32 * 1024)
      @annotation.tailrec
      def loop(): Unit = {
        val n = fis.read(buffer)
        if (n > 0) {
          fn.update(buffer, 0, n)
          loop()
        }
        else ()
      }

      loop()
    })
}
