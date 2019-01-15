package de.hpi.isg.dataprep.preparators.implementation

import scala.util.Try

trait PhoneNumberNormalizer[A] {
	def convert(from: A, to: A)(value: String): Try[String]
	def convert(to: A)(value: String): Try[String]
}

object PhoneNumberNormalizer {
	def convert[A](from: A, to: A)(value: String)(implicit p: PhoneNumberNormalizer[A]): Try[String] =
		p.convert(from, to)(value)

	def convert[A](to: A)(value: String)(implicit p: PhoneNumberNormalizer[A]): Try[String] =
		p.convert(to)(value)
}
