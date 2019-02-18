package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.{AbstractPreparator, Engine}
import org.apache.spark.sql.{Column, Dataset, Row}

/**
  * The DecisionEngine implements greedy approach based on the applicability scores
  */

class DecisionEngine extends Engine {


  /**
    * selects the next best preparator
    *
    * @param allPreps      list of possible preparators
    * @param schemaMapping desired mapping
    * @param data          raw data
    * @param metadata      given metdata
    * @return Some preparator if there is one, otherwise None
    */
  def selectNextPreparation(allPreps: List[Class[_ <: AbstractPreparator]], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: java.util.Set[Metadata]): Option[AbstractPreparator] = {
    if (allPreps.isEmpty || schemaMapping.getCurrentSchema.getAttributes.isEmpty || schemaMapping.hasMapped) return None

    val dataSets = createColumnCombinations(data)

    val max = allPreps
      .flatMap(prepClass => createScoresForCombinations(prepClass, dataSets, schemaMapping, metadata))
      .maxBy { case (prep, score) => score }

    if (max._2 > 0) {
      Some(max._1)
    } else {
      None
    }
  }


  /**
    * creates all possible column combinations as new datasets
    *
    * @param data raw dataset
    * @return Sequence of combinations
    */
  private def createColumnCombinations(data: Dataset[Row]): Seq[Dataset[Row]] = {
    (1 to data.columns.length)
      .flatMap(i => data.columns.map(new Column(_)).combinations(i))
      .map(cols => data.select(cols: _*))
  }

  /**
    * creates new preparator and calculates applicability for every column combination
    *
    * @param prepClass     the given preparator class
    * @param dataSets      the column combinations
    * @param schemaMapping given schema mapping
    * @param metadata      given metadata
    * @return sequence of tuples (preparator, applicabilityScore)
    */
  private def createScoresForCombinations(prepClass: Class[_ <: AbstractPreparator], dataSets: Seq[Dataset[Row]], schemaMapping: SchemaMapping, metadata: java.util.Set[Metadata]): Seq[(AbstractPreparator, Float)] = {
    dataSets.map(data => {
      val prep = prepClass.newInstance()
      (prep, prep.calApplicability(schemaMapping, data, metadata))
    })
  }
}