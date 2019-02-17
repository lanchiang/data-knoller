package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{NANPPhoneNumberFormat, DINPhoneNumberFormat}
import org.scalatest.{FlatSpec, Matchers}

class TaggerSpec extends FlatSpec with Matchers {
	import TaggerInstances._
	import TaggerSyntax._

	"NANPTagger" should "tag the parts of a given phone number according to the North-American-Numbering-Plan" in {
		import NANPPhoneNumberFormat._
		val tags = "0013102461501".tagged(nanpTagger())
		val expected = Map(CountryCode -> "+1", AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")
		tags shouldEqual expected
	}

	"DINTagger" should "tag the parts of a given phone number according to the DIN 5008" in {
		import DINPhoneNumberFormat._
		val tags = "0049/030/1234567".tagged(dinTagger())
		val expected = Map(CountryCode -> "+49", AreaCode -> "030", LineNumber -> "1234567")
		tags shouldEqual expected
	}
}
