package de.hpi.isg.dataprep.preparators.implementation

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.metadata.{LanguageMetadata, LemmatizedMetadata}
import de.hpi.isg.dataprep.metadata.LanguageMetadata.LanguageEnum
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.functions.lit
import org.languagetool.{AnalyzedToken, AnalyzedTokenReadings, JLanguageTool, Language}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by danthe on 26.11.18.
  */
class DefaultLemmatizePreparatorImpl extends AbstractPreparatorImpl with Serializable {

  def lemmatizeString(str: String, language: Class[_ <: Language]): String = {
    val lt = new JLanguageTool(language.newInstance())
    import scala.collection.JavaConverters._
    val analyzedSentences = lt.analyzeText(str).asScala

    val tokens = analyzedSentences.map(
      _.getTokensWithoutWhitespace
    ).reduceOption((a, b) => a ++ b).getOrElse(Array[AnalyzedTokenReadings]()).filter(t => !t.isNonWord && !t.isWhitespace)
    if (tokens.length < 1)
      throw new Exception("Empty field")

    val lemmatized = tokens.map(
      t => {
        if (t.getReadings.size() > 0)
          if(t.getReadings.get(0).getLemma == null) t.getReadings.get(0).getToken else t.getReadings.get(0).getLemma
        else
          t.getToken
      }
    ).mkString(" ")
    lemmatized
  }

  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[LemmatizePreparator]
    val propertyName = preparator.propertyName

    val langMeta = preparator.getPreparation.getPipeline.getMetadataRepository.getMetadata(
      new LanguageMetadata(propertyName, null)).asInstanceOf[LanguageMetadata]

    val realErrors = ListBuffer[PreparationError]()
    var df = dataFrame
    df = df.withColumn(propertyName + "_lemmatized", lit(""))

    val rowEncoder = RowEncoder(df.schema)
    val createdDataset = df.withColumn("iter_index", functions.monotonically_increasing_id()).flatMap(row => {
      val index = row.getAs[Long]("iter_index").asInstanceOf[Int]

      val language = langMeta.getLanguage(index)

      val valIndexTry = Try {
        row.fieldIndex(propertyName)
      }
      val valIndex = valIndexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }
      val operatedValue = row.getAs[String](valIndex)

      val newIndexTry = Try {
        row.fieldIndex(propertyName + "_lemmatized")
      }
      val newIndex = newIndexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }

      val seq = row.toSeq
      val tryConvert = Try {
        val newSeq = seq.zipWithIndex.map { case (value: Any, index: Int) =>
          if (newIndex == index && language != null)
            lemmatizeString(operatedValue, language.getType)
          else if (newIndex == index)
            operatedValue
          else
            value
        }
        val newRow = Row.fromSeq(newSeq)
        newRow
      }

      val trial = tryConvert match {
        case Failure(content) =>
          errorAccumulator.add(new RecordError(operatedValue.toString, content))
          tryConvert
        case Success(content) => tryConvert
      }
      trial.toOption
    })(rowEncoder).drop("iter_index")


    createdDataset.count()
    abstractPreparator.addUpdateMetadata(new LemmatizedMetadata(propertyName))

    new ExecutionContext(createdDataset, errorAccumulator)
  }

}