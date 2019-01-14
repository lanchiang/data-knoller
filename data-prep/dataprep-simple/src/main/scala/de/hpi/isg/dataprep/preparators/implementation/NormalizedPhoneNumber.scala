package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, DINPhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

object NormalizedPhoneNumber {
	import DINPhoneNumberFormatComponent._
	type PhoneNumberComponents = Map[DINPhoneNumberFormatComponent, String]

	def fromMeta(meta: DINPhoneNumberFormat)(value: String): Try[PhoneNumberComponents] = {
		Try {
			(meta.components zip """\d+""".r.findAllIn(value).toList).toMap
		}
	}

	def toMeta(meta: DINPhoneNumberFormat)(components: PhoneNumberComponents): Try[String] = {
		meta.components.foldLeft[Option[String]](Some("")) {
			case (Some(number), ExtensionNumber) => components.get(ExtensionNumber).map(number + "-" + _)
			case (Some(number), component) => components.get(component).map(number + " " + _)
			case (None, _) => None
		}.fold[Try[String]](Failure(new IllegalArgumentException()))(Success.apply)
	}
}
