package de.hpi.isg.dataprep.metadata

sealed trait PhoneNumberFormatComponent

object PhoneNumberFormatComponent {
	case class CountryCode(defaultValue: Option[String] = None) extends PhoneNumberFormatComponent
	case class AreaCode(defaultValue: Option[String] = None) extends PhoneNumberFormatComponent
	case object SpecialNumber extends PhoneNumberFormatComponent
	case object Number extends PhoneNumberFormatComponent
	case object ExtensionNumber extends PhoneNumberFormatComponent
}
