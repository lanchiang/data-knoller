package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType

object PhoneNumberTaggerInstances {
	import PhoneNumberFormatComponentType._

	val defaultTagger: PhoneNumberTagger =
		new PhoneNumberTagger {

			override def tag(parts: List[String]): Map[PhoneNumberFormatComponentType, String] = {
				PhoneNumberFormatComponentType.ordered.foldLeft[(List[String], Map[PhoneNumberFormatComponentType, String])]((parts, Map())) {
					case ((part :: ps, tagged), component) if matches(component)(part) => (ps, tagged + (component -> part))
					case (state, _) => state
				}._2
			}

			private def matches(component: PhoneNumberFormatComponentType)(part: String): Boolean = {
				component match {
					case CountryCode => part.matches("""(\+|00)1""")
					case AreaCode => part.matches("[2-9][0-9][0-9]")
					case CentralOfficeCode => part.matches("[2-9][0-9][0-9]")
					case LineNumber => part.matches("[0-9]{4}")
					case _ => false
				}
			}
		}
}
