package de.hpi.isg.dataprep.preparators.implementation

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator
import edu.stanford.nlp.ling.{CoreAnnotations, CoreLabel}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.functions.lit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by danthe on 26.11.18.
  */
class DefaultLemmatizePreparatorImpl extends PreparatorImpl with Serializable {

  val props = new Properties()
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma")
  @transient var pipeline = new StanfordCoreNLP(props)

  def lemmatizeString(str: String): String = {
    val document = new Annotation(str)
    pipeline.annotate(document)

    val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    val sentenceTokens = sentences.asScala.map(
      _.get(classOf[CoreAnnotations.TokensAnnotation]).asScala
    ).reduceOption((a, b) => a ++ b).getOrElse(mutable.Buffer[CoreLabel]())

    if (sentenceTokens.size < 1)
      throw new Exception("Empty field")
    val lemmatized = sentenceTokens.map(_.get(classOf[CoreAnnotations.LemmaAnnotation])).mkString(" ")
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

      val seq = row.toSeq
      val tryConvert = Try {
        val newSeq = seq.zipWithIndex.map{ case (value:Any, index:Int) =>
          if (remappings.isDefinedAt(index))
            lemmatizeString(remappings(index))
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

  @throws[IOException]
  private def writeObject(oos: ObjectOutputStream) = {
    oos.defaultWriteObject()
  }

  @throws[ClassNotFoundException]
  @throws[IOException]
  private def readObject(ois: ObjectInputStream) = {
    ois.defaultReadObject()
    this.pipeline = new StanfordCoreNLP(props)
  }

}