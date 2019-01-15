package de.hpi.isg.dataprep.metadata

object PhoneNumberFormatCheckerInstances {
	import PhoneNumberFormatComponent._

	implicit val CountryCodeChecker: PhoneNumberFormatChecker[CountryCode.type] =
		new PhoneNumberFormatChecker[CountryCode.type] {
			override def check(value: String)(format: CountryCode.type): Boolean = {
				val countryCodePattern = """((+|00)(\d){1,3})""".r
				value match {
					case countryCodePattern(_*) => true
					case _ => false
				}
			}
		}

		implicit val AreaCodeChecker: PhoneNumberFormatChecker[AreaCode.type] =
			new PhoneNumberFormatChecker[AreaCode.type] {
				override def check(value: String)(format: AreaCode.type): Boolean = {
					val areaCodePattern = """[2-9][0-9][0-9]""".r
					value match {
						case areaCodePattern(_*) => true
						case _ => false
					}
				}
			}
}
