package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, NANPPhoneNumberFormat}

import scala.util.matching.Regex

/**
	* Instances of tagger implementations
	*/
object TaggerInstances {
	/**
		* Tagger instance for the North-American-Numbering-Plan format
		* @param components optional source format
		* @return Tagger instance
		*/
	def nanpTagger(components: Option[List[NANPPhoneNumberFormat]] = None): Tagger[NANPPhoneNumberFormat] =
		new Tagger[NANPPhoneNumberFormat] {
			import NANPPhoneNumberFormat._
			override def tag(value: String): Map[NANPPhoneNumberFormat, String] = {
				new Regex(
					"""(\+1|001)?\D*([2-9][0-9][0-9])?\D*([2-9][0-9][0-9])?\D*([0-9]{4})""",
					"countryCode",
					"areaCode",
					"centralOfficeCode",
					"lineNumber"
				).findFirstMatchIn(value)
					.fold(Map[NANPPhoneNumberFormat, String]()) { parts =>
						components.getOrElse(NANPPhoneNumberFormat.ordered).foldLeft(Map[NANPPhoneNumberFormat, String]()) {
							case (tagged, CountryCode) => Option(parts.group("countryCode")).fold(tagged)(part => tagged + (CountryCode -> part.replace("00", "+")))
							case (tagged, AreaCode) => Option(parts.group("areaCode")).fold(tagged)(part => tagged + (AreaCode -> part))
							case (tagged, CentralOfficeCode) => Option(parts.group("centralOfficeCode")).fold(tagged)(part => tagged + (CentralOfficeCode -> part))
							case (tagged, LineNumber) => Option(parts.group("lineNumber")).fold(tagged)(part => tagged + (LineNumber -> part))
						}
					}
			}
		}

	/**
		* Tagger instance for the DIN 5008 format
		* @param components optional source format
		* @return Tagger instance
		*/
	def dinTagger(components: Option[List[DINPhoneNumberFormat]] = None): Tagger[DINPhoneNumberFormat] =
		new Tagger[DINPhoneNumberFormat] {
			import DINPhoneNumberFormat._
			override def tag(value: String): Map[DINPhoneNumberFormat, String] = {
				new Regex(
					"""(\+49|0049)?\D+([0-9]{2,5})?\D+([1-9][0-9]{1,6})""",
					"countryCode",
					"areaCode",
					"lineNumber"
				).findFirstMatchIn(value)
					.fold(Map[DINPhoneNumberFormat, String]()) { parts =>
						components.getOrElse(DINPhoneNumberFormat.ordered).foldLeft(Map[DINPhoneNumberFormat, String]()) {
							case (tagged, CountryCode) => Option(parts.group("countryCode")).fold(tagged)(part => tagged + (CountryCode -> part.replace("00", "+")))
							case (tagged, AreaCode) => Option(parts.group("areaCode")).fold(tagged)(part => tagged + (AreaCode -> part))
							case (tagged, LineNumber) => Option(parts.group("lineNumber")).fold(tagged)(part => tagged + (LineNumber -> part))
						}
					}
			}
		}
}
