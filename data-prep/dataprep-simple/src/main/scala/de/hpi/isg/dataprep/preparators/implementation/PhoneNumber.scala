package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponent}

import scala.util.{Failure, Success, Try}

/**
	* Phone number
	* @param components Mapping from component type to part of the phone number
	*/
case class PhoneNumber[A](components: Map[A, String]) extends AnyVal {
	import PhoneNumberFormatComponent._

	/**
		* Converting the phone number into a given format
		* @param format Format the phone number is converted to
		* @return Try of the converted phone number
		*/
	def convert(format: PhoneNumberFormat[A]): Try[PhoneNumber[A]] = {
		val convertedComponents = format.components.map {
			case component: Required[A] => components.get(component.componentType).map(component.componentType -> _)
			case component: Optional[A] => components.get(component.componentType).orElse(Option(component.defaultValue)).map(component.componentType -> _)
		}

		if (convertedComponents.exists(_.isEmpty)) Failure(IllegalPhoneNumberFormatException("Missing required component(s)"))
		else Success(PhoneNumber(convertedComponents.flatten.toMap))
	}

	override def toString: String =
		components.values.mkString("-")
}

/**
	* Phone number companion object
	*/
object PhoneNumber {
	import TaggerSyntax._

	def apply[A](value: String)(implicit tagger: Tagger[A]): PhoneNumber[A] =
		new PhoneNumber(value.tagged)
}
