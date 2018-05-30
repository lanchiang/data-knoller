package de.hpi.isg.dataprep.model.metadata.handler

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
trait MetadataExtractor {

  def discoverMetadata(name: String) : Unit
}
