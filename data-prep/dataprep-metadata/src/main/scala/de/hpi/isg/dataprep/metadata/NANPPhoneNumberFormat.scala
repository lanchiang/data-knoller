package de.hpi.isg.dataprep.metadata

/**
	* North-American-Numbering-Plan phone number format
	*/
sealed trait NANPPhoneNumberFormat

object NANPPhoneNumberFormat {
	case object CountryCode extends NANPPhoneNumberFormat
	case object AreaCode extends NANPPhoneNumberFormat
	case object CentralOfficeCode extends NANPPhoneNumberFormat
	case object LineNumber extends NANPPhoneNumberFormat

	val ordered: List[NANPPhoneNumberFormat] = List(
		CountryCode,
		AreaCode,
		CentralOfficeCode,
		LineNumber
	)
}
