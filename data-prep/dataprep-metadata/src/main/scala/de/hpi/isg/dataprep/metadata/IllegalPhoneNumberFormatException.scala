package de.hpi.isg.dataprep.metadata

/**
	* Exception thrown in converting process
	* @param message
	*/
case class IllegalPhoneNumberFormatException(message: String) extends Exception(message)
