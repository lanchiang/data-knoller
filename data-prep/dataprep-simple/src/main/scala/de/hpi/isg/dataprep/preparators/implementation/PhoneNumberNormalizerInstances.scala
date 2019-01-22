package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponent, PhoneNumberFormatComponentType}

import scala.util.{Failure, Success, Try}

object PhoneNumberNormalizerInstances {
	import PhoneNumberFormatComponent._
	import PhoneNumberTaggerSyntax._

	def defaultNormalizer(implicit tagger: PhoneNumberTagger): PhoneNumberNormalizer[PhoneNumberFormat] =
		new PhoneNumberNormalizer[PhoneNumberFormat] {
			override def convert(from: PhoneNumberFormat, to: PhoneNumberFormat)(value: String): Try[String] =
				toMeta(to)((from.components.map(_.componentType) zip split(value)).toMap)

			override def convert(to: PhoneNumberFormat)(value: String): Try[String] =
				toMeta(to)(split(value).tagged)

			private def split(value: String): List[String] =
				"""\d+""".r.findAllIn(value).toList

			private def toMeta(meta: PhoneNumberFormat)(components: Map[PhoneNumberFormatComponentType, String]): Try[String] = {
				val parts = meta.components.map {
					case Required(componentType) => components.get(componentType)
					case Optional(componentType, defaultValue) => Some(components.getOrElse(componentType, defaultValue))
				}

				if (parts.exists(_.isEmpty)) Failure(IllegalPhoneNumberFormatException("Missing required component(s)"))
				else Success(parts.flatten.mkString("-"))
			}
		}
}
