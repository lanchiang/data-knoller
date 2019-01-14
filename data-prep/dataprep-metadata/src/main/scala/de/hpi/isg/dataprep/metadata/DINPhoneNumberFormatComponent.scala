package de.hpi.isg.dataprep.metadata

sealed trait DINPhoneNumberFormatComponent

object DINPhoneNumberFormatComponent {
	case object CountryCode extends DINPhoneNumberFormatComponent
	case object AreaCode extends DINPhoneNumberFormatComponent
	case object SpecialNumber extends DINPhoneNumberFormatComponent
	case object NumberFormat extends DINPhoneNumberFormatComponent
	case object ExtensionNumber extends DINPhoneNumberFormatComponent
}
