package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType

object PhoneNumberTaggerSyntax {
	implicit class Parts(parts: List[String]) {
		def tagged(implicit tagger: PhoneNumberTagger): Map[PhoneNumberFormatComponentType, String] =
			tagger.tag(parts)
	}
}
