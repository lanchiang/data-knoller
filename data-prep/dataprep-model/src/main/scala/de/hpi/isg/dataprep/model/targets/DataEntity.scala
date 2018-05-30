package de.hpi.isg.dataprep.model.targets

import org.apache.spark.sql.DataFrame

/** This class defines the data entity used by error handler to determine the scope of data
  * to be validated.
  *
  * @author Lan Jiang
  * @since 2018/5/28
  */
abstract class DataEntity(dataEntity : List[Target], df: DataFrame) extends Serializable {

  var operatedDataEntity = dataEntity
  var dataFrame = df
}
