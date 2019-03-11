package de.hpi.isg.dataprep.metadata

/**
	* Phone Number Format Component
	* @tparam A Type of the component
	*/
sealed trait PhoneNumberFormatComponent[A] {
	val componentType: A
}

object PhoneNumberFormatComponent {
	case class Required[A](componentType: A) extends PhoneNumberFormatComponent[A]
	case class Optional[A](componentType: A, defaultValue: String) extends PhoneNumberFormatComponent[A]
}
