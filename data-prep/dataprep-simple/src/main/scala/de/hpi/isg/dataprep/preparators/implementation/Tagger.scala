package de.hpi.isg.dataprep.preparators.implementation

/**
	* Tagger
	* @tparam A Type of a tag
	*/
trait Tagger[A] {
	/**
		* Tagging a value
		* @param value Value to be tagged
		* @return Mapping from tag to part in the value
		*/
	def tag(value: String): Map[A, String]
}

/**
	* Tagger companion object
	*/
object Tagger {
	/**
		* Tagging a value
		* @param value Value to be tagged
		* @param tagger Instance of a Tagger implementation
		* @tparam A Type of a tag
		* @return Mapping from tag to part
		*/
	def tag[A](value: String)(implicit tagger: Tagger[A]): Map[A, String] =
		tagger.tag(value)
}
