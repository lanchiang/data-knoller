package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ConversionHelper, DatasetUtils, ExecutionContext}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.{AdaptiveChangeDateFormat, SuggestChangeDateFormat}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2019-04-23
  */
class DefaultSuggestChangeDateFormatImpl extends AbstractPreparatorImpl {

  // Todo: use transform-by-example to derive the correct date format
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SuggestChangeDateFormat]
    val propertyName = preparator.propertyName
    val sourceDatePattern = preparator.sourceDatePattern
    val targetDatePattern = preparator.targetDatePattern

    val intermediate = dataFrame.withColumn(propertyName + "_reformatted", lit(""))
    val rowEncoder = RowEncoder(intermediate.schema)

    val dateClusterPatterns = dataFrame.rdd
            .map(_.getAs[String](propertyName))
            .groupBy(ChangeDateFormatUtils.getSimilarityCriteria)
            .filter(group => group._1 != null)
            .mapValues(clusteredDates => ChangeDateFormatUtils.extractClusterDatePattern(clusteredDates.toList))
            .collect()
            .toMap[PatternCriteria, Option[LocalePattern]]

    val createdDataset = intermediate.flatMap(row => {
      val index = DatasetUtils.getFieldIndexByPropertyNameSafe(row, propertyName)
      val operatedValue = row.getAs[String](propertyName)

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length-index-1)

      // Todo: now the formatted values overwrite the original column
      val tryConvert = Try{
        if (sourceDatePattern.isDefined) {
          val newSeq = (forepart :+ ConversionHelper.toDate(operatedValue, sourceDatePattern.get, targetDatePattern.get)) ++ backpart
          val newRow = Row.fromSeq(newSeq)
          newRow
        } else {
          val localePatternOption: Option[LocalePattern] = dateClusterPatterns(ChangeDateFormatUtils.getSimilarityCriteria(operatedValue))
          val newSeq = (forepart :+ ChangeDateFormatUtils.formatToTargetPattern(operatedValue, targetDatePattern.get, localePatternOption)) ++ backpart
          val newRow = Row.fromSeq(newSeq)
          newRow
        }
      }

      val convertOption = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
          tryConvert
        }
        case Success(content) => tryConvert
      }

      convertOption.toOption
    })(rowEncoder)
    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}