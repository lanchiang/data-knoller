package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponentType}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class PhoneNumberSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import TaggerInstances._

	"PhoneNumber" should "create an instance from a given number and a format" in {
		val number = "310-246-1501"
		val tagger = phoneNumberTagger(List(AreaCode, CentralOfficeCode, LineNumber))
		val expected = Map[PhoneNumberFormatComponentType, String](AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")

		PhoneNumber(number)(tagger).components shouldEqual expected
	}

	it should "create an instance from a given number and a tagger" in {
		val number = "3102461501"
		val tagger = phoneNumberTagger(PhoneNumberFormatComponentType.ordered)
		val expected = Map[PhoneNumberFormatComponentType, String](AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")

		PhoneNumber(number)(tagger).components shouldEqual expected
	}

	it should "convert a phone number to another format" in {
		val number = PhoneNumber(Map[PhoneNumberFormatComponentType, String](AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501"))

		val validFormat = PhoneNumberFormat(List(CountryCode.optional("+1"), AreaCode.optional("200"), CentralOfficeCode.required))
		val invalidFormat = PhoneNumberFormat(List(CountryCode.required, AreaCode.required, CentralOfficeCode.required, LineNumber.required))

		val valid = number.convert(validFormat)
		val invalid = number.convert(invalidFormat)

		val validExpected = Success {
			PhoneNumber {
				Map[PhoneNumberFormatComponentType, String](
					CountryCode -> "+1",
					AreaCode -> "310",
					CentralOfficeCode -> "246"
				)
			}
		}

		val invalidExpected = Failure {
			IllegalPhoneNumberFormatException("Missing required component(s)")
		}


		valid shouldEqual validExpected
		invalid shouldEqual invalidExpected
	}

	it should "convert a phone number to correctly formatted string" in {
		PhoneNumber {
			Map[PhoneNumberFormatComponentType, String](
				CountryCode -> "+1",
				AreaCode -> "310",
				CentralOfficeCode -> "246",
				LineNumber -> "1501"
			)
		}.toString shouldEqual "+1-310-246-1501"
	}
}
