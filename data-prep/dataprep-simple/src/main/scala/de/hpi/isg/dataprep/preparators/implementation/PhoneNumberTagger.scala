package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.metadata.PhoneNumberFormatComponentType

trait PhoneNumberTagger {
	def tag(parts: List[String]): Map[PhoneNumberFormatComponentType, String]
}

object PhoneNumberTagger {
	def tag(parts: List[String])(implicit tagger: PhoneNumberTagger): Map[PhoneNumberFormatComponentType, String] =
		tagger.tag(parts)
}
