package de.hpi.isg.dataprep.preparators.implementation

import scala.util.Try

object PhoneNumberNormalizerSyntax {
	implicit class Meta[A,B](meta: A) {
		def encode(value: String)(implicit p: PhoneNumberNormalizer[A,B]): Try[Map[B, String]] =
			p.fromMeta(meta)(value)

		def decode(components: Map[B, String])(implicit p: PhoneNumberNormalizer[A,B]): Try[String] =
			p.toMeta(meta)(components)
	}
}
