package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponent

trait PhoneNumberTagger {
	def tag(parts: List[String]): Map[PhoneNumberFormatComponent, String]
}

object PhoneNumberTagger {
	def tag(parts: List[String])(implicit tagger: PhoneNumberTagger): Map[PhoneNumberFormatComponent, String] =
		tagger.tag(parts)
}
