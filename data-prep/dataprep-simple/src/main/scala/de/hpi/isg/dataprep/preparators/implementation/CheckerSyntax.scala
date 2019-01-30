package de.hpi.isg.dataprep.preparators.implementation

/**
	* Checker syntax
	*/
object CheckerSyntax {
	implicit class Value(value: String) {
		/**
			* Checking if the value conforms a given format
			* @param format Format the value is checked against
			* @param checker Instance of a checker implementation
			* @tparam A Type of the format
			* @return Whether or not the value matches the format
			*/
		def matchesFormat[A](format: A)(implicit checker: Checker[A]): Boolean =
			checker.check(format)(value)
	}
}
