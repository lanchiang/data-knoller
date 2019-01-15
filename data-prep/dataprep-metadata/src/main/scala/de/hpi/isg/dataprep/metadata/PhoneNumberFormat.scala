package de.hpi.isg.dataprep.metadata

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata

abstract class PhoneNumberFormat extends Metadata("phoneNumber") {
	override def checkMetadata(metadataRepository: MetadataRepository): Unit = ()
	override def equalsByValue(metadata: Metadata): Boolean = this.equals(metadata)
}
