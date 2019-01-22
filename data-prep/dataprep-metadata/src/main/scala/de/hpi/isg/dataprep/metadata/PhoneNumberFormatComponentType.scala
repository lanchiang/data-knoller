package de.hpi.isg.dataprep.metadata

sealed trait PhoneNumberFormatComponentType {
	import PhoneNumberFormatComponent._
	def required: Required = Required(this)
	def optional(defaultValue: String): Optional = Optional(this, defaultValue)
}

object PhoneNumberFormatComponentType {
	case object CountryCode extends PhoneNumberFormatComponentType
	case object AreaCode extends PhoneNumberFormatComponentType
	case object CentralOfficeCode extends PhoneNumberFormatComponentType
	case object LineNumber extends PhoneNumberFormatComponentType

	val ordered: List[PhoneNumberFormatComponentType] = List(
		CountryCode,
		AreaCode,
		CentralOfficeCode,
		LineNumber
	)
}
