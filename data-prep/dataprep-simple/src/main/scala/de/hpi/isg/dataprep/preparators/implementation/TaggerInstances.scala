package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType

/**
	* Instances of tagger implementations
	*/
object TaggerInstances {
	import CheckerSyntax._

	implicit def phoneNumberTagger(implicit checker: Checker[PhoneNumberFormatComponentType]): Tagger[PhoneNumberFormatComponentType] =
		new Tagger[PhoneNumberFormatComponentType] {
			override def tag(parts: Seq[String]): Map[PhoneNumberFormatComponentType, String] = {
				PhoneNumberFormatComponentType.ordered.foldLeft[(Seq[String], Map[PhoneNumberFormatComponentType, String])]((parts, Map())) {
					case ((part :: ps, tagged), component) if part.matchesFormat(component) => (ps, tagged + (component -> part))
					case (state, _) => state
				}._2
			}
		}
}
