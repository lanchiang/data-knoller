package de.hpi.isg.dataprep.cases

/**
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
abstract class BooleanType {}


case class OneZero() extends BooleanType

case class TrueFalse() extends BooleanType