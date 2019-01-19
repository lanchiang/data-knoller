package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponent

object PhoneNumberTaggerInstances {
	import PhoneNumberFormatComponent._

	val defaultTagger: PhoneNumberTagger =
		new PhoneNumberTagger {

			val components = List(
				CountryCode(),
				AreaCode(),
				SpecialNumber,
				Number,
				ExtensionNumber
			)

			override def tag(parts: List[String]): Map[PhoneNumberFormatComponent, String] = {
				parts.foldLeft[(List[PhoneNumberFormatComponent], Map[PhoneNumberFormatComponent, String])]((components, Map())) {
					case ((component :: cs, tagged), part) if part.matchesFormat(component) => (cs, tagged + (component -> part))
					case ((cs, tagged), _) => (cs, tagged)
				}._2
			}
		}
}
