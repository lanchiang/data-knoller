package de.hpi.isg.dataprep.cases

/**
  * This is the set of case classes representing property data type.
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
abstract class DataType

case class INTEGER() extends DataType

case class STRING() extends DataType

case class DOUBLE() extends DataType

case class DATE(datePattern: DatePattern) extends DataType
