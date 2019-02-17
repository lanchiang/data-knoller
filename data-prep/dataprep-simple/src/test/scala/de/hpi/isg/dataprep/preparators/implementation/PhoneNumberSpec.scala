package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponent, NANPPhoneNumberFormat}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class PhoneNumberSpec extends FlatSpec with Matchers {
	import NANPPhoneNumberFormat._
	import PhoneNumberFormatComponent._
	import TaggerInstances._

	"PhoneNumber" should "create an instance from a given number and a format" in {
		val number = "310-246-1501"
		val tagger = nanpTagger(Some(List(AreaCode, CentralOfficeCode, LineNumber)))
		val expected = Map[NANPPhoneNumberFormat, String](AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")

		PhoneNumber(number)(tagger).components shouldEqual expected
	}

	it should "create an instance from a given number and a tagger" in {
		val number = "3102461501"
		val tagger = nanpTagger()
		val expected = Map[NANPPhoneNumberFormat, String](AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")

		PhoneNumber(number)(tagger).components shouldEqual expected
	}

	it should "convert a phone number to another format" in {
		val number = PhoneNumber(Map[NANPPhoneNumberFormat, String](
			AreaCode -> "310",
			CentralOfficeCode -> "246",
			LineNumber -> "1501"
		))

		val validFormat = PhoneNumberFormat[NANPPhoneNumberFormat](List(
			Optional(CountryCode, "+1"),
			Optional(AreaCode, "200"),
			Required(CentralOfficeCode)
		))
		val invalidFormat = PhoneNumberFormat[NANPPhoneNumberFormat](List(
			Required(CountryCode),
			Required(AreaCode),
			Required(CentralOfficeCode),
			Required(LineNumber)
		))

		val valid = number.convert(validFormat)
		val invalid = number.convert(invalidFormat)

		val validExpected = Success {
			PhoneNumber {
				Map[NANPPhoneNumberFormat, String](
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
			Map[NANPPhoneNumberFormat, String](
				CountryCode -> "+1",
				AreaCode -> "310",
				CentralOfficeCode -> "246",
				LineNumber -> "1501"
			)
		}.toString shouldEqual "+1-310-246-1501"
	}
}
