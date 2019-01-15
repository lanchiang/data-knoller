package de.hpi.isg.dataprep.metadata

object PhoneNumberFormatCheckerInstances {
	import PhoneNumberFormatComponent._

	implicit val PhoneNumberFormatChecker: PhoneNumberFormatChecker[PhoneNumberFormat] =
		new PhoneNumberFormatChecker[PhoneNumberFormat] {
			override def check(value: String)(format: PhoneNumberFormat): Boolean = {
				val parts = """\d+""".r.findAllIn(value)
				format.components.forall { component =>
					parts.exists { part =>
						component match {
							case CountryCode => part.matches("""(+|00)(\d){1,3}""")
							case AreaCode => part.matches("""[2-9][0-9][0-9]""")
						}
					}
				}
			}
		}

	implicit val PhoneNumberFormatComponentChecker: PhoneNumberFormatChecker[PhoneNumberFormatComponent] =
		new PhoneNumberFormatChecker[PhoneNumberFormatComponent] {
			override def check(value: String)(format: PhoneNumberFormatComponent): Boolean = {
				format match {
					case CountryCode => value.matches("""(+|00)(\d){1,3}""")
					case AreaCode => value.matches("""[2-9][0-9][0-9]""")
				}
			}
		}
}
