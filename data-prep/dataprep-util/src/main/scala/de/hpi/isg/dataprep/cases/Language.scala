package de.hpi.isg.dataprep.cases

/**
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
abstract class Language

case class EN() extends Language

case class DE() extends Language

object Language {

  def getLanguage(language: Language): String = {
    language match {
      case EN() => "English"
      case DE() => "Deutsch"
    }
  }
}
