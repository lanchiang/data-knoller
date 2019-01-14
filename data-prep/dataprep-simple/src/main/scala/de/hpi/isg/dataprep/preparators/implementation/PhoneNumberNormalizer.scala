package de.hpi.isg.dataprep.preparators.implementation

import scala.util.Try

trait PhoneNumberNormalizer[A,B] {
	def fromMeta(meta: A)(value: String): Try[Map[B, String]]
	def toMeta(meta: A)(components: Map[B, String]): Try[String]
}

object PhoneNumberNormalizer {
	def fromMeta[A,B](meta: A)(value: String)(implicit p: PhoneNumberNormalizer[A,B]): Try[Map[B, String]] =
		p.fromMeta(meta)(value)

	def toMeta[A,B](meta: A)(components: Map[B, String])(implicit p: PhoneNumberNormalizer[A,B]): Try[String] =
		p.toMeta(meta)(components)
}
