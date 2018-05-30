package de.hpi.isg.dataprep.model.provenance

import de.hpi.isg.dataprep.model.targets.Provenance

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
class ProvenanceRepository {

  private var repository: List[Provenance] = List()

  def addProvenance(provenance: Provenance): Unit = {
    repository = repository :+ provenance
  }

}
