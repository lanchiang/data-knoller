package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, DINPhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

object PhoneNumberNormalizerInstances {
	implicit val DINNormalizer: PhoneNumberNormalizer[DINPhoneNumberFormat, DINPhoneNumberFormatComponent] =
		new PhoneNumberNormalizer[DINPhoneNumberFormat, DINPhoneNumberFormatComponent] {
			import DINPhoneNumberFormatComponent._

			override def fromMeta(meta: DINPhoneNumberFormat)(value: String): Try[Map[DINPhoneNumberFormatComponent, String]] =
				Try {
					(meta.components zip """\d+""".r.findAllIn(value).toList).toMap
				}

			override def toMeta(meta: DINPhoneNumberFormat)(components: Map[DINPhoneNumberFormatComponent, String]): Try[String] = {
				meta.components.foldLeft[Option[String]](Some("")) {
					case (Some(number), ExtensionNumber) => components.get(ExtensionNumber).map(number + "-" + _)
					case (Some(number), component) => components.get(component).map(number + " " + _)
					case (None, _) => None
				}.fold[Try[String]](Failure(new IllegalArgumentException()))(Success.apply)
			}
		}
}
