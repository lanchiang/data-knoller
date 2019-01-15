package de.hpi.isg.dataprep.metadata

object PhoneNumberFormatCheckerSyntax {
	implicit class Value(value: String) {
		def matchesFormat[A](format: A)(implicit c: PhoneNumberFormatChecker[A]): Boolean =
			c.check(value)(format)
	}
}
