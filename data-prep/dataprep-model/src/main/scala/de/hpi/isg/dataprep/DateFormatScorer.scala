package de.hpi.isg.dataprep

import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.buffer.IntBuffer
import org.nd4j.linalg.cpu.nativecpu.NDArray

import scala.io.Source

class DateFormatScorer(modelFile: String = "model.hdf5", vocabularyFile: String = "vocab.csv") extends Serializable {
  private var vocabulary: Map[String, Int] = _
  loadVocabulary()

  private var model: MultiLayerNetwork = _
  loadModel()

  /**
    * Score a single date with the model. This method wraps the dl4j api for the loaded model.
    * @param date the date that is scored
    * @return the score as float since the underlying data type is float as well
    */
  def score(date: String): Float = {
    val tokens = tokenize(date)
    val data = new NDArray(new IntBuffer(tokens.toArray))
    val prediction = model.output(data)
    // the prediction result is only a single float (encapsulated in an INDArray)
    prediction.getFloat(0L)
  }

  private def tokenize(date: String): List[Int] = {
    date
      .map(element => vocabulary.getOrElse(element.toString, oovTokenIndex()))
      .toList
  }

  private def oovTokenIndex(): Int = vocabulary.getOrElse("oov", 0)

  private def loadVocabulary(): Unit = {
    val file = Source.fromURL(getClass.getResource(s"/date_format/$vocabularyFile"))
    vocabulary = file
        .getLines()
        .map { line =>
          val Array(index, token) = line.split(",", 2)
          (token, index.toInt)
        }.toMap
  }

  private def loadModel(): Unit = {
    val modelPath = getClass.getResource(s"/date_format/$modelFile").getPath
    model = KerasModelImport.importKerasSequentialModelAndWeights(modelPath)
  }

}
