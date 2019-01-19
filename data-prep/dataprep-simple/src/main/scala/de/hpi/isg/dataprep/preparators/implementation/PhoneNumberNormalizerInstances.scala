package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

object PhoneNumberNormalizerInstances {
	import PhoneNumberFormatComponent._
	import PhoneNumberTaggerSyntax._

	def defaultNormalizer(implicit tagger: PhoneNumberTagger): PhoneNumberNormalizer[PhoneNumberFormat] =
		new PhoneNumberNormalizer[PhoneNumberFormat] {
			override def convert(from: PhoneNumberFormat, to: PhoneNumberFormat)(value: String): Try[String] =
				fromMeta(from)(value) flatMap toMeta(to)

			override def convert(to: PhoneNumberFormat)(value: String): Try[String] =
				fromUnknown(value) flatMap toMeta(to)

			private def split(value: String): List[String] =
				"""\d+""".r.findAllIn(value).toList

			private def fromMeta(meta: PhoneNumberFormat)(value: String): Try[Map[PhoneNumberFormatComponent, String]] =
				Try((meta.components zip split(value)).toMap)

			private def fromUnknown(value: String): Try[Map[PhoneNumberFormatComponent, String]] =
				Try {
					val components = split(value).tagged

					val defaultComponents = components.keys.filterNot(components.contains).flatMap {
						case CountryCode(Some(defaultValue)) => Some(CountryCode(Some(defaultValue)) -> defaultValue)
						case AreaCode(Some(defaultValue)) => Some(AreaCode(Some(defaultValue)) -> defaultValue)
						case _ => None
					}

					components ++ defaultComponents
				}

			private def toMeta(meta: PhoneNumberFormat)(components: Map[PhoneNumberFormatComponent, String]): Try[String] = {
				meta.components.foldLeft[Option[String]](Some("")) {
					case (Some(number), ExtensionNumber) => components.get(ExtensionNumber).map(number + "-" + _)
					case (Some(number), component) => components.get(component).map(number + " " + _)
					case (None, _) => None
				}.fold[Try[String]](Failure(new IllegalArgumentException()))(Success.apply)
			}
		}
}
