package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponent, PhoneNumberFormatComponentType}

import scala.util.{Failure, Success, Try}

/**
	* Phone number
	* @param components Mapping from component type to part of the phone number
	*/
case class PhoneNumber(components: Map[PhoneNumberFormatComponentType, String]) extends AnyVal {
	import PhoneNumberFormatComponent._

	/**
		* Converting the phone number into a given format
		* @param format Format the phone number is converted to
		* @return Try of the converted phone number
		*/
	def convert(format: PhoneNumberFormat): Try[PhoneNumber] = {
		val convertedComponents = format.components.map {
			case Required(componentType) => components.get(componentType).map(componentType -> _)
			case Optional(componentType, defaultValue) => components.get(componentType).orElse(Option(defaultValue)).map(componentType -> _)
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

	def apply(value: String)(implicit tagger: Tagger[PhoneNumberFormatComponentType]): PhoneNumber =
		new PhoneNumber(value.tagged)
}
