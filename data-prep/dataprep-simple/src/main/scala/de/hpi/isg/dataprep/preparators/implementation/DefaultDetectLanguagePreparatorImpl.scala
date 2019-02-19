package de.hpi.isg.dataprep.preparators.implementation

import java.util

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.metadata.LanguageMetadata.LanguageEnum
import de.hpi.isg.dataprep.metadata.{LanguageMetadata, UnsupportedLanguageException}
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.DetectLanguagePreparator
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.languagetool.language.LanguageIdentifier

/**
  * Created by znnr on 14.01.19.
  */
class DefaultDetectLanguagePreparatorImpl extends AbstractPreparatorImpl with Serializable {

  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[DetectLanguagePreparator]
    val propertyName = preparator.propertyName

    val detector = new LanguageIdentifier(2000)
    val langPerChunk = dataFrame.select(propertyName)
      .collect().grouped(preparator.chunkSize).zipWithIndex.map(rowChunkWithId => {
      val rowChunk = rowChunkWithId._1
      val index = rowChunkWithId._2

      val textChunk = rowChunk.map(_.getAs[String](0)).mkString(" ")
      val lang = detector.detectLanguage(textChunk)

      try {
        val enumLang = if (lang != null) LanguageEnum.langForClass(lang.getClass) else null
        (index.asInstanceOf[Integer], enumLang)
      } catch {
        case e: UnsupportedLanguageException =>
          errorAccumulator.add(new PropertyError(propertyName, e))
          (index.asInstanceOf[Integer], null)
      }
    }).toMap
    val javaMap = new util.HashMap[Integer, LanguageEnum]()
    langPerChunk.foreach(entry => javaMap.put(entry._1, entry._2))

    val langMetadata = new LanguageMetadata(propertyName, javaMap, preparator.chunkSize)
    abstractPreparator.addUpdateMetadata(langMetadata)

    new ExecutionContext(dataFrame, errorAccumulator)
  }

}