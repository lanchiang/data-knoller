package de.hpi.isg.dataprep.preparators.implementation

import scala.util.Try

object PhoneNumberNormalizerSyntax {
	implicit class PhoneNumber[A](value: String) {
		def converted(from: A, to: A)(implicit p: PhoneNumberNormalizer[A]): Try[String] =
			p.convert(from, to)(value)

		def converted(to: A)(implicit p: PhoneNumberNormalizer[A]): Try[String] =
			p.convert(to)(value)
	}
}
