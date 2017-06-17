package org.bykn.hyperbuild

import cats.{~>, Applicative, Monad, MonadError}
import java.io.File

sealed abstract class HasFingerprint[M[_], A] { self =>
  def fingerprint(a: A)(implicit app: Applicative[M]): M[Fingerprint]
  def on[B](fn: B => A): HasFingerprint[M, B]
  def onM[B](fn: B => M[A])(implicit M: Monad[M]): HasFingerprint[M, B]
  def transform[N[_]](nt: M ~> N): HasFingerprint[N, A]
}

object HasFingerprint extends HasFingerprint0 {
  private case class FromFnM[M[_], A](fn: A => M[Fingerprint]) extends HasFingerprint[M, A] {
    def fingerprint(a: A)(implicit app: Applicative[M])  = fn(a)
    def on[B](fnb: B => A) = FromFnM(fnb.andThen(fn))
    def onM[B](bma: B => M[A])(implicit M: Monad[M]) = FromFnM { b: B => M.flatMap(bma(b))(fn) }
    def transform[N[_]](nt: M ~> N): HasFingerprint[N, A] =
      FromFnM(fn.andThen(nt[Fingerprint]))
  }

  private case class FromFn[M[_], A](fn: A => Fingerprint) extends HasFingerprint[M, A] {
    def fingerprint(a: A)(implicit app: Applicative[M])  = app.pure(fn(a))
    def on[B](fnb: B => A) = FromFn(fnb.andThen(fn))
    def onM[B](bma: B => M[A])(implicit M: Monad[M]) = FromFnM { b: B => M.map(bma(b))(fn) }
    def transform[N[_]](nt: M ~> N): HasFingerprint[N, A] =
      FromFn[N, A](fn)
  }

  def fromM[M[_], A](fn: A => M[Fingerprint]): HasFingerprint[M, A] =
    FromFnM(fn)

  def from[M[_], A](fn: A => Fingerprint): HasFingerprint[M, A] =
    FromFn(fn)

  implicit def fileFingerPrint[M[_]](implicit me: MonadError[M, Throwable]): HasFingerprint[M, File] =
    fromM(Fingerprint.fromFile[M](_))
}

private[hyperbuild] abstract class HasFingerprint0 {
  implicit def fromSer[M[_], A: Serialization]: HasFingerprint[M, A] =
    HasFingerprint.from(Fingerprint.of(_))
}