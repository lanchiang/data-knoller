package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponent, PhoneNumberFormatComponentType}

import scala.util.{Failure, Success, Try}

/**
	* Phone number
	* @param values Mapping from component type to part of the phone number
	*/
case class PhoneNumber(values: Map[PhoneNumberFormatComponentType, String]) extends AnyVal {
	import PhoneNumberFormatComponent._

	/**
		* Converting the phone number into a given format
		* @param format Format the phone number is converted to
		* @return Try of the converted phone number
		*/
	def convert(format: PhoneNumberFormat): Try[PhoneNumber] = {
		val components = format.components.map {
			case Required(componentType) => values.get(componentType).map(componentType -> _)
			case Optional(componentType, defaultValue) => values.get(componentType).orElse(Option(defaultValue)).map(componentType -> _)
		}

		if (components.exists(_.isEmpty)) Failure(IllegalPhoneNumberFormatException("Missing required component(s)"))
		else Success(PhoneNumber(components.flatten.toMap))
	}

	override def toString: String =
		values.values.mkString("-")
}

/**
	* Phone number companion object
	*/
object PhoneNumber {
	import TaggerSyntax._

	def apply(format: PhoneNumberFormat)(value: String): PhoneNumber = {
		new PhoneNumber((format.components.map(_.componentType) zip value.split("""\D+""")).toMap)
	}

	def apply(value: String)(implicit tagger: Tagger[PhoneNumberFormatComponentType]): PhoneNumber = {
		new PhoneNumber(value.split("""\D+""").toList.tagged)
	}
}
