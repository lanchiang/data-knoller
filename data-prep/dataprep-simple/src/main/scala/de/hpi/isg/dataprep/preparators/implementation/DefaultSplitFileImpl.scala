package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.SplitFile
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitFileImpl extends AbstractPreparatorImpl {
  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {

    val preparator = abstractPreparator.asInstanceOf[SplitFile]
    val fileSeparator = preparator.fileSeparator


    if (fileSeparator == "") {
      val (foundSeparator, sepConfidence) = ConversionHelper.findUnknownFileSeparator(dataFrame)
      if (sepConfidence >= 1) {
        val splitData = ConversionHelper.splitFileBySeparator(foundSeparator, dataFrame)
      }
      if (dataFrame.columns.length >= 2) {
        val splitData = ConversionHelper.splitFileByEmptyValues(dataFrame)
        val splitByValuesAfterEmpty = ConversionHelper.splitFileByNewValuesAfterEmpty(dataFrame)
        if (splitByValuesAfterEmpty.count > splitData.count) {
          return new ExecutionContext(splitData, errorAccumulator)
        } else {
          return new ExecutionContext(splitByValuesAfterEmpty, errorAccumulator)
        }
      }
    } else {
      val splitDataSets = ConversionHelper.splitFileBySeparator(fileSeparator, dataFrame)
      return new ExecutionContext(splitDataSets, errorAccumulator)
    }


    new ExecutionContext(dataFrame, errorAccumulator)
  }
}
