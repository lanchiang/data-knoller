package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType
import org.scalatest.{FlatSpec, Matchers}

class TaggerSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import CheckerInstances._
	import TaggerInstances._
	import TaggerSyntax._

	"PhoneNumberTagger" should "tag a given list of phone number parts" in {
		val tags = List("001", "310", "246", "1501").tagged
		val expected = Map(CountryCode -> "001", AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")
		tags shouldEqual expected
	}

	it should "return an empty Map if the phone number parts do not match" in {
		val tags = List("01", "01", "1990").tagged
		tags.isEmpty shouldBe true
	}
}
