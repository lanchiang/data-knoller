package de.hpi.isg.dataprep.metadata

sealed trait PhoneNumberFormatComponent

object PhoneNumberFormatComponent {
	case object CountryCode extends PhoneNumberFormatComponent
	case object AreaCode extends PhoneNumberFormatComponent
	case object SpecialNumber extends PhoneNumberFormatComponent
	case object NumberFormat extends PhoneNumberFormatComponent
	case object ExtensionNumber extends PhoneNumberFormatComponent
}
