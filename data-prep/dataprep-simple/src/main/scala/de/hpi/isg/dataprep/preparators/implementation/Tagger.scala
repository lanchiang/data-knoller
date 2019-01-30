package de.hpi.isg.dataprep.preparators.implementation

/**
	* Tagger
	* @tparam A Type of a tag
	*/
trait Tagger[A] {
	/**
		* Tagging parts of a sequence
		* @param parts Parts to be tagged
		* @return Mapping from tag to part
		*/
	def tag(parts: Seq[String]): Map[A, String]
}

/**
	* Tagger companion object
	*/
object Tagger {
	/**
		* Tagging parts of a sequence
		* @param parts Parts to be tagged
		* @param tagger Instance of a Tagger implementation
		* @tparam A Type of a tag
		* @return Mapping from tag to part
		*/
	def tag[A](parts: Seq[String])(implicit tagger: Tagger[A]): Map[A, String] =
		tagger.tag(parts)
}
