package org.bykn.hyperbuild

import com.twitter.bijection.JavaSerializationInjection
import cats.{~>, Applicative, Functor, Monad, MonadError}
import java.io.File
import scala.reflect.ClassTag

sealed abstract class HasFingerprint[M[_], A] { self =>
  def fingerprint(a: A)(implicit app: Applicative[M]): M[Fingerprint]
  def on[B](fn: B => A): HasFingerprint[M, B]
  def onM[B](fn: B => M[A])(implicit M: Monad[M]): HasFingerprint[M, B]
  def transform[N[_]](nt: M ~> N): HasFingerprint[N, A]
  def andThen(fp: Fingerprint => Fingerprint)(implicit M: Functor[M]): HasFingerprint[M, A]
}

object HasFingerprint extends HasFingerprint0 {
  private case class FromFnM[M[_], A](fn: A => M[Fingerprint]) extends HasFingerprint[M, A] {
    def fingerprint(a: A)(implicit app: Applicative[M])  = fn(a)
    def on[B](fnb: B => A) = FromFnM(fnb.andThen(fn))
    def onM[B](bma: B => M[A])(implicit M: Monad[M]) = FromFnM { b: B => M.flatMap(bma(b))(fn) }
    def transform[N[_]](nt: M ~> N): HasFingerprint[N, A] =
      FromFnM(fn.andThen(nt[Fingerprint]))
    def andThen(fp: Fingerprint => Fingerprint)(implicit M: Functor[M]): HasFingerprint[M, A] =
      FromFnM { a => M.map(fn(a))(fp) }
  }

  private case class FromFn[M[_], A](fn: A => Fingerprint) extends HasFingerprint[M, A] {
    def fingerprint(a: A)(implicit app: Applicative[M])  = app.pure(fn(a))
    def on[B](fnb: B => A) = FromFn(fnb.andThen(fn))
    def onM[B](bma: B => M[A])(implicit M: Monad[M]) = FromFnM { b: B => M.map(bma(b))(fn) }
    def transform[N[_]](nt: M ~> N): HasFingerprint[N, A] =
      FromFn[N, A](fn)
    def andThen(fp: Fingerprint => Fingerprint)(implicit M: Functor[M]): HasFingerprint[M, A] =
      FromFn(fn.andThen(fp))
  }

  def fingerprint[M[_]: Applicative, A](a: A)(implicit hfp: HasFingerprint[M, A]): M[Fingerprint] =
    hfp.fingerprint(a)

  def fromM[M[_], A](fn: A => M[Fingerprint]): HasFingerprint[M, A] =
    FromFnM(fn)

  def from[M[_], A](fn: A => Fingerprint): HasFingerprint[M, A] =
    FromFn(fn)

  implicit def nothingHasFingerprint[M[_]]: HasFingerprint[M, Nothing] =
    from[M, Nothing] { n: Nothing => n }

  implicit def fileFingerPrint[M[_]](implicit me: MonadError[M, Throwable]): HasFingerprint[M, File] =
    fromM(Fingerprint.fromFile[M](_))

  implicit def optionHasFingerprint[M[_], A](implicit app: Applicative[M], hfpa: HasFingerprint[M, A]): HasFingerprint[M, Option[A]] =
    hfpa match {
      case FromFn(fn) => FromFn[M, Option[A]] {
        case None => Fingerprint.ofString("scala.None")
        case Some(a) => Fingerprint.combine(Fingerprint("scala.Some"), fn(a))
      }
      case FromFnM(fn) => FromFnM[M, Option[A]] {
        case None => app.pure(Fingerprint.ofString("scala.None"))
        case Some(a) => app.map(fn(a))(Fingerprint.combine(Fingerprint("scala.Some"), _))
      }
    }
  /**
   * this serializes the item using java serialization and uses the hash
   */
  def serializable[M[_], A <: Serializable: ClassTag]: HasFingerprint[M, A] = {
    val ser = JavaSerializationInjection[A]
    HasFingerprint.fromSer(ser)
  }
}

private[hyperbuild] abstract class HasFingerprint0 {
  implicit def fromSer[M[_], A: Serialization]: HasFingerprint[M, A] =
    HasFingerprint.from(Fingerprint.of(_))
}
