package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ExplodeArray
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

class DefaultExplodeArrayImpl extends PreparatorImpl {
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
    val preparator = abstractPreparator.asInstanceOf[ExplodeArray]
    val colToSplit = preparator.propertyName

    val splittedCols = preparator.columnNames match {
      case Some(names) => names
      case None => (1 to dataFrame.head.getAs[mutable.WrappedArray[_]](colToSplit).length).map(i => colToSplit + s"_$i").toArray
    }


    val newColumns = dataFrame.columns.flatMap(columnName =>
      if (columnName != colToSplit) Seq(col(columnName))
      else splittedCols.zipWithIndex.map(t => getValueOfSeq(t._2, colToSplit, t._1))
    )

    new ExecutionContext(dataFrame.select(newColumns: _*), errorAccumulator)
  }

  def getValueOfSeq(id: Int, arrayColumnName: String, newColumnName: String): Column = col(arrayColumnName)(id).as(newColumnName)
}
