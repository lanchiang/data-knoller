package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitFile
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitFileImpl extends PreparatorImpl{
  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {

    val preparator = abstractPreparator.asInstanceOf[SplitFile]
    val fileSeparator = preparator.fileSeparator

    if(fileSeparator == ""){
      val (foundSeparator, sepConfidence) = ConversionHelper.findUnknownFileSeparator(dataFarme)
      if(sepConfidence >= 1){
        val splitData = ConversionHelper.splitFileBySeparator(foundSeparator, dataFrame)
      }
      // TODO: implement heuristic for splitting dataframes as good as we can - if there is no separator
    }else{
      val splitDataSets = ConversionHelper.splitFileBySeparator(fileSeparator, dataFrame)
    }


    new ExecutionContext(dataFrame, errorAccumulator)
  }
}
