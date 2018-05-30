package de.hpi.isg.dataprep.model.error

/** This class defines the data entity used by error handler to determine the scope of data
  * to be validated.
  * @author Lan Jiang
  * @since 2018/5/28
  */
object DataEntity extends Enumeration {

  type DataEntity = Value
  val SINGLECOLUMN, SINGLEROW, MULTIPLECOLUMNS, MULTIPLEROWS, TABLE, NONE = Value
}
