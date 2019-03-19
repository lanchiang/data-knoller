package de.hpi.isg.dataprep.schema

import de.hpi.isg.dataprep.ConversionHelper
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.util.Try

/**
  * This class provides the utility functions that manipulate the schema of a spark [[org.apache.spark.sql.DataFrame]]
  *
  * @author Lan Jiang
  * @since 2018/9/17
  */
object SchemaUtils {

  /**
    * Use the given field name and data type to update the corresponding field in the schema.
    *
    * @param schema
    * @param fieldName
    * @param dataType the new data type.
    * @return
    */
  def updateSchema(schema: StructType, fieldName: String, dataType: DataType): StructType = {
    val newSchema = StructType(schema.map(column => {
      if (column.name == fieldName) {
        StructField(column.name, dataType, column.nullable, column.metadata)
      }
      else {
        column
      }
    }))
    newSchema
  }

  /**
    * Move the column in the last to the specified position.
    *
    * @param dataFrame is the operated [[DataFrame]]
    * @param position is the new position
    * @return
    */
  def lastToNewPosition(dataFrame: DataFrame, position: Int): DataFrame = {
    val columns = dataFrame.columns

    val headPart = columns.slice(0, position)
    val tailPart = columns.slice(position, columns.length - 1)
    val reorderedSchema = headPart ++ columns.slice(columns.length - 1, columns.length) ++ tailPart

    dataFrame.select(reorderedSchema.map(column => col(column)): _*)
  }

  /**
    * Judge whether the specified position is inside the range of this [[DataFrame]]
    * @param dataFrame is the operated [[DataFrame]]
    * @param position is the specified position
    * @return true if the position is inside the range of the schema
    */
  def positionIsInTheRange(dataFrame: DataFrame, position: Int): Boolean = {
    position >=0 && position < dataFrame.schema.length
  }

  /**
    * Create a [[Row]] that change the value at the specified position of the current [[Row]] to the new value while keep the rest part unchanged.
    *
    * @param row is the current [[Row]]
    * @param index is the specified position
    * @param newVal is the new value
    * @return the [[Try]] of the [[Row]]
    */
  def createRow(row: Row, index: Int, newVal: String): Try[Row] = {
    val seq = row.toSeq
    val forepart = seq.take(index)
    val backpart = seq.takeRight(row.length - index - 1)

    val tryConvert = Try {
      val newSeq = (forepart :+ newVal) ++ backpart
      val newRow = Row.fromSeq(newSeq)
      newRow
    }
    tryConvert
  }

  /**
    * Create a [[Row]] that change the value at the specified position of the current [[Row]] to the new value while keep the rest part unchanged.
    *
    * @param row is the current [[Row]]
    * @param index is the specified position
    * @param newVal is the new value
    * @return the [[Try]] of the [[Row]]
    */
  def createOneRow(row: Row, index: Int, newVal: String): Row = {
    val seq = row.toSeq
    val forepart = seq.take(index)
    val backpart = seq.takeRight(row.length - index - 1)

    val newSeq = (forepart :+ newVal) ++ backpart
    val newRow = Row.fromSeq(newSeq)
    newRow
  }
}
