package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType

import scala.util.matching.Regex

/**
	* Instances of tagger implementations
	*/
object TaggerInstances {
	import PhoneNumberFormatComponentType._

	def phoneNumberTagger(components: List[PhoneNumberFormatComponentType]): Tagger[PhoneNumberFormatComponentType] =
		new Tagger[PhoneNumberFormatComponentType] {
			override def tag(value: String): Map[PhoneNumberFormatComponentType, String] = {
				new Regex(
					"""(\+1|001)?\D*([2-9][0-9][0-9])?\D*([2-9][0-9][0-9])?\D*([0-9]{4})""",
					"countryCode",
					"areaCode",
					"centralOfficeCode",
					"lineNumber"
				).findFirstMatchIn(value)
					.fold(Map[PhoneNumberFormatComponentType, String]()) { parts =>
						components.foldLeft(Map[PhoneNumberFormatComponentType, String]()) {
							case (tagged, CountryCode) => Option(parts.group("countryCode")).fold(tagged)(part => tagged + (CountryCode -> part.replace("00", "+")))
							case (tagged, AreaCode) => Option(parts.group("areaCode")).fold(tagged)(part => tagged + (AreaCode -> part))
							case (tagged, CentralOfficeCode) => Option(parts.group("centralOfficeCode")).fold(tagged)(part => tagged + (CentralOfficeCode -> part))
							case (tagged, LineNumber) => Option(parts.group("lineNumber")).fold(tagged)(part => tagged + (LineNumber -> part))
						}
					}
			}
		}
}
