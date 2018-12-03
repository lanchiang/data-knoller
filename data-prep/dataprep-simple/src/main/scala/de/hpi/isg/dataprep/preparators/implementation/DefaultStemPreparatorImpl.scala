package de.hpi.isg.dataprep.preparators.implementation

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.util.Properties

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.StemPreparator
import de.hpi.isg.dataprep.util.Stemmer
import edu.stanford.nlp.ling.{CoreAnnotations, CoreLabel}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by danthe on 26.11.18.
  */
class DefaultStemPreparatorImpl extends PreparatorImpl with Serializable {

  val props = new Properties()
  props.setProperty("annotators", "tokenize")
  @transient var pipeline = new StanfordCoreNLP(props)

  def stemString(str: String): String = {
    val document = new Annotation(str)
    pipeline.annotate(document)

    val tokens = document.get(classOf[CoreAnnotations.TokensAnnotation]).asScala
    if (tokens.size < 1)
      throw new Exception("Empty field")

    val s = new Stemmer
    val stemmed = tokens.map(t => s.stem(t.word())).mkString(" ")
    stemmed
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
    val preparator = abstractPreparator.asInstanceOf[StemPreparator]
    val propertyNames = preparator.propertyNames

    var df = dataFrame
    for (name <- propertyNames) {
      df = df.withColumn(name + "_stemmed", lit(""))
    }
    val rowEncoder = RowEncoder(df.schema)
    val createdDataset = df.flatMap(row => {

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
          row.fieldIndex(propertyName + "_stemmed")
        }
        val index = indexTry match {
          case Failure(content) => throw content
          case Success(content) => content
        }
        (index, operatedValue)
      }).toMap

      val seq = row.toSeq
      val tryConvert = Try {
        val newSeq = seq.zipWithIndex.map { case (value:Any, index:Int) =>
          if (remappings.isDefinedAt(index))
            stemString(remappings(index))
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
