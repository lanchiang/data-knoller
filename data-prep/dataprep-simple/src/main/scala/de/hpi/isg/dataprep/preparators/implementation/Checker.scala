package de.hpi.isg.dataprep.preparators.implementation

/**
	* Checker Trait
	* @tparam A Type of the format
	*/
trait Checker[A] {
	/**
		* Checking a value if it conforms a given format
		* @param format Format the value is checked against
		* @param value Value to be checked
		* @return Whether or not the value matches the format
		*/
	def check(format: A)(value: String): Boolean
}

/**
	* Checker companion object
	*/
object Checker {
	/**
		* Checking a value if it conforms a given format
		* @param format Format the value is checked against
		* @param value Value to be checked
		* @param checker Instance of a checker implementation
		* @tparam A Type of the format
		* @return Whether or not the value matches the format
		*/
	def check[A](format: A)(value: String)(implicit checker: Checker[A]): Boolean =
		checker.check(format)(value)
}
