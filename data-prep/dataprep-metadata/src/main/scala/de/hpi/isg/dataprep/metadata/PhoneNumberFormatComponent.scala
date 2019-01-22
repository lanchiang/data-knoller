package de.hpi.isg.dataprep.metadata

sealed trait PhoneNumberFormatComponent {
	val componentType: PhoneNumberFormatComponentType
}

object PhoneNumberFormatComponent {
	case class Required(componentType: PhoneNumberFormatComponentType) extends PhoneNumberFormatComponent
	case class Optional(componentType: PhoneNumberFormatComponentType, defaultValue: String) extends PhoneNumberFormatComponent
}
