package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitAttribute
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitAttributeImpl._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitAttributeImpl extends PreparatorImpl {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: DataFrame,
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitAttribute]
    val propertyName = preparator.propertyName
    val startLeft = preparator.startLeft

    val separator = preparator.separator match {
      case null => "\\" + countOccurrences(dataFrame, propertyName).maxBy(_._1)._1._1.toString
      case s => s
    }

    val times = preparator.times match {
      case -1 => None
      case value => Some(value)
    }

    if (!dataFrame.columns.contains(propertyName)) {
      errorAccumulator.add(new PropertyError(propertyName, new PreparationHasErrorException("Column name does not exist.")))
      new ExecutionContext(dataFrame, errorAccumulator)
    } else {
      split(dataFrame, errorAccumulator, propertyName, separator, startLeft, times)
    }
  }

  def split(dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError], propertyName: String, separator: String, startLeft: Boolean, times: Option[Int]): ExecutionContext = {
    val newDf = dataFrame.withColumnRenamed(propertyName, propertyName + "_old").withColumn(propertyName, split(separator, times, startLeft)(col(propertyName + "_old"))).drop(col(propertyName + "_old"))
    new ExecutionContext(newDf, errorAccumulator)
  }

  def split(separator: String, times: Option[Int] = None, startLeft: Boolean = true): UserDefinedFunction =
    udf((s: String) => {
      val splitString = if (startLeft) s.split(separator) else s.reverse.split(separator).reverse
      if (times.isEmpty) splitString else splitString.take(times.get)
    })

  def countOccurrences(dataFrame: DataFrame, columnName: String): Map[(Char, Int), Int] = {
    dataFrame.rdd.map(_.getAs[String](columnName))
      .flatMap(countSeparatorInString)
      .map(i => (i, 1))
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap
  }

  def countSeparatorInString: String => List[(Char, Int)] = (string: String) => {
    string.filter(PotentialSeparator.contains(_))
      .groupBy(i => i)
      .map(char => (char._1, char._2.length))
      .toList
  }
}

object DefaultSplitAttributeImpl {
  val PotentialSeparator = Array(',', '|', ';', '!', '\t', '_', '-')
}