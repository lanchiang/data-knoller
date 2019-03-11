package de.hpi.isg.dataprep.metadata

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata

/**
	* Phone Number Format
	* @param components Components of the Format
	* @tparam A Type of the components
	*/
case class PhoneNumberFormat[A](components: List[PhoneNumberFormatComponent[A]]) extends Metadata("phoneNumber") {
	override def checkMetadata(metadataRepository: MetadataRepository): Unit = ()
	override def equalsByValue(metadata: Metadata): Boolean = this.equals(metadata)
}
