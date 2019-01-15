package de.hpi.isg.dataprep.metadata

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata

case class PhoneNumberFormat(components: List[PhoneNumberFormatComponent]) extends Metadata("phoneNumber") {
	override def checkMetadata(metadataRepository: MetadataRepository): Unit = ()
	override def equalsByValue(metadata: Metadata): Boolean = this.equals(metadata)
}

object PhoneNumberFormat {
	def check[A](value: String)(format: A)(implicit c: PhoneNumberFormatChecker[A]): Boolean =
		c.check(value)(format)
}
