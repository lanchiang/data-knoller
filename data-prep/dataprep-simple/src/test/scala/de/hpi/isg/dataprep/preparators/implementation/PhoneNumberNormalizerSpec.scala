package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{IllegalPhoneNumberFormatException, PhoneNumberFormat, PhoneNumberFormatComponentType}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class PhoneNumberNormalizerSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import PhoneNumberNormalizerInstances._
	import PhoneNumberNormalizerSyntax._
	import PhoneNumberTaggerInstances._

	"PhoneNumberNormalizer" should "convert a phone number from a source format to a target format" in {
		val from = PhoneNumberFormat(List(AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val to = PhoneNumberFormat(List(AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val phoneNumbers = List("212-219/2777", "770/441--0291", "310-828-7937.", "415/929-").map(_.converted(from, to)(defaultNormalizer(defaultTagger)))
		val expected = List(Success("212-219-2777"), Success("770-441-0291"), Success("310-828-7937"), Failure(IllegalPhoneNumberFormatException("Missing required component(s)")))

		phoneNumbers shouldEqual expected
	}

	it should "handle default values" in {
		val from = PhoneNumberFormat(List(AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val to = PhoneNumberFormat(List(CountryCode.optional("+1"), AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val phoneNumbers = List("212-219/2777", "770/441--0291", "310-828-7937.", "415/929-").map(_.converted(from, to)(defaultNormalizer(defaultTagger)))
		val expected = List(Success("+1-212-219-2777"), Success("+1-770-441-0291"), Success("+1-310-828-7937"), Failure(IllegalPhoneNumberFormatException("Missing required component(s)")))

		phoneNumbers shouldEqual expected
	}

	it should "handle missing source formats" in {
		val to = PhoneNumberFormat(List(CountryCode.optional("+1"), AreaCode.required, CentralOfficeCode.required, LineNumber.required))
		val phoneNumbers = List("212-219/2777", "770/441--0291", "310-828-7937.", "415/929-").map(_.converted(to)(defaultNormalizer(defaultTagger)))
		val expected = List(Success("+1-212-219-2777"), Success("+1-770-441-0291"), Success("+1-310-828-7937"), Failure(IllegalPhoneNumberFormatException("Missing required component(s)")))

		phoneNumbers shouldEqual expected
	}
}
