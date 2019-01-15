package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, DINPhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

object PhoneNumberNormalizerInstances {
	implicit val DINNormalizer: PhoneNumberNormalizer[DINPhoneNumberFormat] =
		new PhoneNumberNormalizer[DINPhoneNumberFormat] {
			import DINPhoneNumberFormatComponent._

			override def convert(from: DINPhoneNumberFormat, to: DINPhoneNumberFormat)(value: String): Try[String] = {
				fromMeta(from)(value) flatMap toMeta(to)
			}

			override def convert(to: DINPhoneNumberFormat)(value: String): Try[String] = {
				fromValue(value) flatMap toMeta(to)
			}
			
			private def fromMeta(meta: DINPhoneNumberFormat)(value: String): Try[Map[DINPhoneNumberFormatComponent, String]] =
				Try {
					(meta.components zip """\d+""".r.findAllIn(value).toList).toMap
				}

			private def toMeta(meta: DINPhoneNumberFormat)(components: Map[DINPhoneNumberFormatComponent, String]): Try[String] = {
				meta.components.foldLeft[Option[String]](Some("")) {
					case (Some(number), ExtensionNumber) => components.get(ExtensionNumber).map(number + "-" + _)
					case (Some(number), component) => components.get(component).map(number + " " + _)
					case (None, _) => None
				}.fold[Try[String]](Failure(new IllegalArgumentException()))(Success.apply)
			}

			private def fromValue(value: String): Try[Map[DINPhoneNumberFormatComponent, String]] =
				Try {
					"""\d+""".r.findAllIn(value).foldLeft(Map[DINPhoneNumberFormatComponent, String]()) {
						case (components, part) if part.startsWith("+") => components + (CountryCode -> part)
					}
				}
		}
}
