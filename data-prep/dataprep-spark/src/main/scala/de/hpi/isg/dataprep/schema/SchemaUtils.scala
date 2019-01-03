package de.hpi.isg.dataprep.schema

import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
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

}
