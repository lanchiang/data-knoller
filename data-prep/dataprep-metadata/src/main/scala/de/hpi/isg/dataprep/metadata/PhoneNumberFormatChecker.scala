package de.hpi.isg.dataprep.metadata

trait PhoneNumberFormatChecker[A] {
	def check(value: String)(format: A): Boolean
}
