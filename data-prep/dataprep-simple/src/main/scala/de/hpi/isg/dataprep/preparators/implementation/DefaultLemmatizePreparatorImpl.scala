package de.hpi.isg.dataprep.preparators.implementation

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.metadata.LanguageMetadata
import de.hpi.isg.dataprep.metadata.LanguageMetadata.LanguageEnum
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator
import edu.stanford.nlp.ling.{CoreAnnotations, CoreLabel}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.StringUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.functions.lit
import org.languagetool.{AnalyzedToken, AnalyzedTokenReadings, JLanguageTool, Language}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by danthe on 26.11.18.
  */
class DefaultLemmatizePreparatorImpl extends AbstractPreparatorImpl with Serializable {

  def lemmatizeString(str: String, language: Class[_ <: Language]): String = {
    val lt = new JLanguageTool(language.newInstance())
    val analyzedSentences = lt.analyzeText(str).asScala

    val tokens = analyzedSentences.map(
      _.getTokensWithoutWhitespace
    ).reduceOption((a, b) => a ++ b).getOrElse(Array[AnalyzedTokenReadings]()).filter(t => !t.isNonWord && !t.isWhitespace)
    if (tokens.length < 1)
      throw new Exception("Empty field")

    val lemmatized = tokens.map(
      t => if (t.getReadings.size() > 0) t.getReadings.get(0).getLemma else t.getToken
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
    val propertyNames = preparator.propertyNames

    var propLang = new mutable.HashMap[String, Class[_ <: Language]]()
    propertyNames.foreach(propertyName => {
      // TODO: This is so broken...
      val langMeta = preparator.getPreparation.getPipeline.getMetadataRepository.getMetadata(
        new LanguageMetadata(propertyName, LanguageMetadata.LanguageEnum.ANY)).asInstanceOf[LanguageMetadata]
      // TODO: as we won't apply the preparator if there ist no metadata, the following line is redundant IMO
      val language = if(langMeta.getLanguage == null) LanguageEnum.ENGLISH.getType else langMeta.getLanguage.getType
      propLang.put(propertyName, language)
    })

    val realErrors = ListBuffer[PreparationError]()
    var df = dataFrame
    for (name <- propertyNames) {
      df = df.withColumn(name + "_lemmatized", lit(""))
    }
    val rowEncoder = RowEncoder(df.schema)
    val createdDataset = df.flatMap(row => {
      //
      val remappings = propertyNames.map(propertyName => {
        val valIndexTry = Try {
          row.fieldIndex(propertyName)
        }
        val valIndex = valIndexTry match {
          case Failure(content) => throw content
          case Success(content) => content
        }
        val operatedValue = row.getAs[String](valIndex)

        val indexTry = Try {
          row.fieldIndex(propertyName + "_lemmatized")
        }
        val index = indexTry match {
          case Failure(content) => throw content
          case Success(content) => content
        }
        (index, operatedValue)
      }).toMap

      var langMapping = new mutable.HashMap[Int, Class[_ <: Language]]()
      propertyNames.foreach(propertyName => {
        val indexTry = Try {
          row.fieldIndex(propertyName + "_lemmatized")
        }
        val index = indexTry match {
          case Failure(content) => throw content
          case Success(content) => content
        }
        langMapping.put(index, propLang(propertyName))
      })

      val seq = row.toSeq
      val tryConvert = Try {
        val newSeq = seq.zipWithIndex.map { case (value: Any, index: Int) =>
          if (remappings.isDefinedAt(index))
            lemmatizeString(remappings(index), langMapping(index))
          else
            value
        }
        val newRow = Row.fromSeq(newSeq)
        newRow
      }

      val trial = tryConvert match {
        case Failure(content) =>
          errorAccumulator.add(new RecordError(remappings.values.mkString(","), content))
          tryConvert
        case Success(content) => tryConvert
      }
      trial.toOption
    })(rowEncoder)
    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

}