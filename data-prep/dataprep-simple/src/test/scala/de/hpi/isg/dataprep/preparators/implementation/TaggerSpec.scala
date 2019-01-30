package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType
import org.scalatest.{FlatSpec, Matchers}

class TaggerSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import TaggerInstances._
	import TaggerSyntax._

	"PhoneNumberTagger" should "tag the parts of a given phone number" in {
		val tags = "0013102461501".tagged(phoneNumberTagger(PhoneNumberFormatComponentType.ordered))
		val expected = Map(CountryCode -> "+1", AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")
		tags shouldEqual expected
	}
}
