package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatComponentType}

/**
	* Instances of checker implementations
	*/
object CheckerInstances {
	import PhoneNumberFormatComponentType._
	import CheckerSyntax._

	implicit val phoneFormatComponentTypeChecker: Checker[PhoneNumberFormatComponentType] =
		new Checker[PhoneNumberFormatComponentType] {
			override def check(format: PhoneNumberFormatComponentType)(value: String): Boolean = {
				format match {
					case CountryCode => value.matches("""(\+|00)1""")
					case AreaCode => value.matches("[2-9][0-9][0-9]")
					case CentralOfficeCode => value.matches("[2-9][0-9][0-9]")
					case LineNumber => value.matches("[0-9]{4}")
				}
			}
		}

	implicit val phoneFormatChecker: Checker[PhoneNumberFormat] =
		new Checker[PhoneNumberFormat] {
			override def check(format: PhoneNumberFormat)(value: String): Boolean = {
				value.split("-") match {
					case parts if parts.size == format.components.size =>
						(format.components.map(_.componentType) zip parts).forall {
							case (component, part) => part.matchesFormat(component)
						}
					case _ => false
				}
			}
		}
}
