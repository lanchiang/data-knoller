package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponent

object PhoneNumberTaggerSyntax {
	implicit class Parts(parts: List[String]) {
		def tagged(implicit tagger: PhoneNumberTagger): Map[PhoneNumberFormatComponent, String] =
			tagger.tag(parts)
	}
}
