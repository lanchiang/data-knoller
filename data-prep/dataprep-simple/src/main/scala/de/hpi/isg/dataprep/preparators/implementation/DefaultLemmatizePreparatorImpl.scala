package de.hpi.isg.dataprep.preparators.implementation

import java.util.Properties

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  * Created by danthe on 26.11.18.
  */
class DefaultLemmatizePreparatorImpl extends PreparatorImpl {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame        contains the intermediate dataset
    * @param errorAccumulator is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[LemmatizePreparator]
    val propertyName = preparator.propertyName

    val rowEncoder = RowEncoder(dataFrame.schema)
    val createdDataset = dataFrame.flatMap(row => {
      val indexTry = Try{row.fieldIndex(propertyName)}
      val index = indexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }
      val operatedValue = row.getAs[String](index)

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length-index-1)
      val tryConvert = Try{
        val newSeq = (forepart :+ lemmatizeString(operatedValue)) ++ backpart
        val newRow = Row.fromSeq(newSeq)
        newRow
      }

      val trial = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
          tryConvert
        }
        case Success(content) => tryConvert
      }
      trial.toOption
    })(rowEncoder)

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  val props = new Properties()
  props.setProperty("annotators", "lemma")
  val pipeline = new StanfordCoreNLP(props)

  private def lemmatizeString(str: String): String ={

    val document = new Annotation(str)
    pipeline.annotate(document)

    val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    if (sentences.size() != 1)
      throw new Exception("Field empty or more than one sentence supplied")
    val tokens = sentences.get(0).get(classOf[CoreAnnotations.TokensAnnotation])
    if (tokens.size() != 1)
      throw new Exception("Field empty or more than one token supplied")
    val lemmatized = tokens.get(0).get(classOf[CoreAnnotations.LemmaAnnotation])

    lemmatized

  }

}
