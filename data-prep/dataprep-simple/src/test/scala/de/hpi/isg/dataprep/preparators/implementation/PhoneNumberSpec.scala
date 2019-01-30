package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponentType}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class PhoneNumberSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import CheckerInstances.phoneFormatComponentTypeChecker
	import TaggerInstances.phoneNumberTagger

	"PhoneNumber" should "create an instance from a given number and a format" in {
		val number = "310-246-1501"
		val format = PhoneNumberFormat(List(AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val expected = Map[PhoneNumberFormatComponentType, String](
			AreaCode -> "310",
			CentralOfficeCode -> "246",
			LineNumber -> "1501"
		)

		PhoneNumber(format)(number).values shouldEqual expected
	}

	it should "create an instance from a given number and a tagger" in {
		val number = "310-246-1501"
		val expected = Map[PhoneNumberFormatComponentType, String](
			AreaCode -> "310",
			CentralOfficeCode -> "246",
			LineNumber -> "1501"
		)

		PhoneNumber(number).values shouldEqual expected
	}

	it should "convert a phone number to another format" in {
		val number = PhoneNumber("310-246-1501")

		val validFormat = PhoneNumberFormat {
			List(CountryCode.optional("+1"), AreaCode.optional("200"), CentralOfficeCode.required)
		}

		val invalidFormat = PhoneNumberFormat {
			List(CountryCode.required, AreaCode.required, CentralOfficeCode.required, LineNumber.required)
		}

		val validResult = number.convert(validFormat)
		val invalidResult = number.convert(invalidFormat)

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


		validResult shouldEqual validExpected
		invalidResult shouldEqual invalidExpected
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
