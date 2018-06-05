package de.hpi.isg.dataprep

import org.apache.spark.sql.DataFrame


/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
object SparkPreparators {

  def renameColumn(dataframe: DataFrame, columnName: String, newColumnName: String): DataFrame = {
    dataframe.withColumnRenamed(columnName, newColumnName)
  }

}
