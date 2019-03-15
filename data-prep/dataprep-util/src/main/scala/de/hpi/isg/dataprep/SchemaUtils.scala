package de.hpi.isg.dataprep

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  *
  * @author Lan Jiang
  * @since 2018/8/21
  */
// Todo: this class can be merged with the SchemaUtils under dataprep-spark module.
object SchemaUtils {

  def positionValidation(positionInSchema: Int, schema: Array[String]): Boolean = {
    if (positionInSchema < 0 || positionInSchema > schema.length) {
      false
    } else {
      true
    }
  }

  /**
    * Move the column in the last to the specified position.
    *
    * @param dataFrame
    * @param position
    * @return
    */
  def lastToNewPosition(dataFrame: DataFrame, position: Int): DataFrame = {
    val columns = dataFrame.columns

    val headPart = columns.slice(0, position)
    val tailPart = columns.slice(position, columns.length - 1)
    val reorderedSchema = headPart ++ columns.slice(columns.length - 1, columns.length) ++ tailPart

    dataFrame.select(reorderedSchema.map(column => col(column)): _*)
  }
}
