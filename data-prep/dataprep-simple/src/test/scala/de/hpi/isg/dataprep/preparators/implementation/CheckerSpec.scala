package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatComponentType}
import org.scalatest.{FlatSpec, Matchers}

class CheckerSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import CheckerInstances._
	import CheckerSyntax._

	"Checker" should "check if a phone number part matches a given format" in {
		val validValues = Map[PhoneNumberFormatComponentType, String](
			CountryCode -> "001",
			AreaCode -> "310",
			CentralOfficeCode -> "246",
			LineNumber -> "1501"
		)

		val invalidValues = Map[PhoneNumberFormatComponentType, String](
			CountryCode -> "0173",
			AreaCode -> "000",
			CentralOfficeCode -> "01011990",
			LineNumber -> ""
		)

		validValues.forall { case (component, part) => part.matchesFormat(component) } shouldBe true
		invalidValues.exists { case (component, part) => part.matchesFormat(component) } shouldBe false
	}

	it should "check if a phone number matches a given format" in {
		val validPhoneNumber = "310-246-1501"
		val invalidPhoneNumber = "01-01-1990"
		val format = PhoneNumberFormat(List(AreaCode.required, CentralOfficeCode.required, LineNumber.required))

		validPhoneNumber.matchesFormat(format) shouldBe true
		invalidPhoneNumber.matchesFormat(format) shouldBe false
	}
}
