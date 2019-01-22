package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType
import org.scalatest.{FlatSpec, Matchers}

class PhoneNumberTaggerSpec extends FlatSpec with Matchers {
	import PhoneNumberFormatComponentType._
	import PhoneNumberTaggerInstances._
	import PhoneNumberTaggerSyntax._

	"PhoneNumberTagger" should "tag a given list of phone number parts" in {
		val tags = List("001", "310", "246", "1501").tagged(defaultTagger)
		val expected = Map(CountryCode -> "001", AreaCode -> "310", CentralOfficeCode -> "246", LineNumber -> "1501")
		tags shouldEqual expected
	}
}
