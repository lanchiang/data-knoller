package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDateFormatImpl
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}
import org.nd4j.linalg.factory.Nd4j
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class ChangeDateFormat(val propertyName: String,
                       val sourceDatePattern: Option[DatePatternEnum] = None,
                       val targetDatePattern: Option[DatePatternEnum] = None) extends AbstractPreparator {
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
    * metadata into both prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = ListBuffer[Metadata]()
    val toChange = ListBuffer[Metadata]()
    prerequisites += new PropertyDataType(propertyName, PropertyType.STRING)

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }

    sourceDatePattern match {
      case Some(pattern) => prerequisites += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None =>
    }

    targetDatePattern match {
      case Some(pattern) => toChange += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None => throw new ParameterNotSpecifiedException("Target pattern not specified!")
    }

    this.prerequisites.addAll(prerequisites.toList.asJava)
    this.updates.addAll(toChange.toList.asJava)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    /*
    val modelPath = getClass.getResource("/model_emb3_epochs2.hdf5").getPath
    val model = KerasModelImport.importKerasSequentialModelAndWeights(modelPath)
    val in = .. INDArray with input data
    val output = model.output(in)
    */
    // THIS IS A PLACEHOLDER IMPLEMENTATION (for task 3 of Axel Stebner, Jan Ehmueller)
    val impl = this.impl.asInstanceOf[DefaultChangeDateFormatImpl]
    val schema = dataset.schema
    val maxCount = dataset.count.toFloat

    // try to convert every column and find the one with the highest score
    val results = schema.toList.map { column =>
      val columnName = column.name
      column.dataType match {
        // we can only convert string columns
        case StringType =>
          val converted = impl.convertDateInDataset(
            dataset,
            RowEncoder(schema),
            None,
            columnName,
            this.targetDatePattern.getOrElse(DatePatternEnum.DayMonthYear)
          )
          converted.count
        // any other column type results in a score of 0
        case _ => 0
      }
    }

    // choose the highest score and normalize it with the number of rows in the original dataset
    if (results.nonEmpty && maxCount > 0) {
      results.max.toFloat / maxCount
    } else {
      0.0f
    }
  }
}

object ChangeDateFormat {
  def apply(
             propertyName: String,
             sourceDatePattern: DatePatternEnum,
             targetDatePattern: DatePatternEnum
           ): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, Option(sourceDatePattern), Option(targetDatePattern))
  }

  def apply(propertyName: String, targetDatePattern: DatePatternEnum): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, targetDatePattern = Option(targetDatePattern))
  }
}
