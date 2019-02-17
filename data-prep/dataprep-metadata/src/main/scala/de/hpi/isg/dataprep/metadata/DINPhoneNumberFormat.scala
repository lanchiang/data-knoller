package de.hpi.isg.dataprep.metadata

/**
	* DIN 5008 phone number format
	*/
sealed trait DINPhoneNumberFormat

object DINPhoneNumberFormat {
	case object CountryCode extends DINPhoneNumberFormat
	case object AreaCode extends DINPhoneNumberFormat
	case object LineNumber extends DINPhoneNumberFormat

	val ordered: List[DINPhoneNumberFormat] = List(
		CountryCode,
		AreaCode,
		LineNumber
	)
}
