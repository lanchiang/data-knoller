package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatCheckerInstances, PhoneNumberFormatCheckerSyntax, PhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

object PhoneNumberNormalizerInstances {
	implicit val DINNormalizer: PhoneNumberNormalizer[PhoneNumberFormat] =
		new PhoneNumberNormalizer[PhoneNumberFormat] {
			import PhoneNumberFormatComponent._
			import PhoneNumberFormatCheckerInstances._
			import PhoneNumberFormatCheckerSyntax._

			override def convert(from: PhoneNumberFormat, to: PhoneNumberFormat)(value: String): Try[String] =
				fromMeta(from)(value) flatMap toMeta(to)

			override def convert(to: PhoneNumberFormat)(value: String): Try[String] =
				fromValue(value) flatMap toMeta(to)

			private def fromMeta(meta: PhoneNumberFormat)(value: String): Try[Map[PhoneNumberFormatComponent, String]] =
				Try {
					(meta.components zip """\d+""".r.findAllIn(value).toList).toMap
				}

			private def toMeta(meta: PhoneNumberFormat)(components: Map[PhoneNumberFormatComponent, String]): Try[String] = {
				meta.components.foldLeft[Option[String]](Some("")) {
					case (Some(number), ExtensionNumber) => components.get(ExtensionNumber).map(number + "-" + _)
					case (Some(number), component) => components.get(component).map(number + " " + _)
					case (None, _) => None
				}.fold[Try[String]](Failure(new IllegalArgumentException()))(Success.apply)
			}

			private def fromValue(value: String): Try[Map[PhoneNumberFormatComponent, String]] =
				Try {
					"""\d+""".r.findAllIn(value).foldLeft(Map[PhoneNumberFormatComponent, String]()) {
						case (components, part) if part.matchesFormat(CountryCode) => components + (CountryCode -> part)
						case (components, part) if part.matchesFormat(AreaCode) => components + (AreaCode -> part)
					}
				}
		}
}
