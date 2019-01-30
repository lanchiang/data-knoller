package de.hpi.isg.dataprep.preparators.implementation

/**
	* Tagger syntax
	*/
object TaggerSyntax {
	implicit class Value(value: String) {
		/**
			* Tagging a value
			* @param tagger Instance of a Tagger implementation
			* @tparam A Type of a tag
			* @return Mapping from tag to part
			*/
		def tagged[A](implicit tagger: Tagger[A]): Map[A, String] =
			tagger.tag(value)
	}
}
