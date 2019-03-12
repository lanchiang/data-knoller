package de.hpi.isg.dataprep

import java.net.URL

import org.deeplearning4j.nn.modelimport.keras.KerasModelImport
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.nd4j.linalg.api.buffer.IntBuffer
import org.nd4j.linalg.cpu.nativecpu.NDArray

import scala.io.Source

/**
  * This class scores dates with a Keras deep learning model.
  * @param modelFile name of the model file in the date_format directory in the project resources
  * @param vocabularyFile name of the vocabulary file in the date_format directory in the project resources
  */
class DateScorer(modelFile: String = "model.hdf5", vocabularyFile: String = "vocab.csv") extends Serializable {
  var vocabulary: Map[String, Int] = _
  loadVocabulary()

  private var model: MultiLayerNetwork = _
  loadModel()

  /**
    * Score a single date with the model. This method wraps the dl4j api for the loaded model.
    * @param date the date that is scored
    * @return the score as float since the underlying data type is float as well
    */
  def score(date: String): Float = {
    // dl4j can't handle inputs with size 1 (i.e., only one character)
    // those inputs will also not be a date. therefore we just return 0.0
    if (date.length <= 1) {
      return 0.0f
    }

    val tokens = tokenize(date)
    val data = new NDArray(new IntBuffer(tokens.toArray))
    val prediction = model.output(data)
    // the prediction result is only a single float (encapsulated in an INDArray)
    prediction.getFloat(0L)
  }

  /**
    * Tokenizes a date string into a list of char indices. These indices are the input for the scoring model.
    * @param date the input date string
    * @return a list of character indices
    */
  def tokenize(date: String): List[Int] = {
    date
      .map(element => vocabulary.getOrElse(element.toString, oovTokenIndex()))
      .toList
  }

  /**
    * The index of the oov (out of vocabulary) token. The oov token itself should be "oov".
    * @return the index of the oov token
    */
  def oovTokenIndex(): Int = vocabulary.getOrElse("oov", 0)

  /**
    * Wrapper method that uses the vocabulary file specified at object creation.
    */
  def loadVocabulary(): Unit = {
    loadVocabulary(getClass.getResource(s"/date_format/$vocabularyFile"))
  }

  /**
    * Loads a CSV vocabulary file in the format "index,token".
    * @param path URL to the vocabulary file
    */
  def loadVocabulary(path: URL): Unit = {
    val file = Source.fromURL(path)
    vocabulary = file
        .getLines()
        .map { line =>
          val Array(index, token) = line.split(",", 2)
          (token, index.toInt)
        }.toMap
  }

  /**
    * Wrapper method that uses the model file specified at object creation.
    */
  def loadModel(): Unit = {
    loadModel(getClass.getResource(s"/date_format/$modelFile"))
  }

  /**
    * Loads a Sequential Keras model (hdf5) with DL4J.
    * @param path URL to the model file
    */
  def loadModel(path: URL): Unit = {
    // since we only predict we don't need a training config
    model = KerasModelImport.importKerasSequentialModelAndWeights(path.getPath, false)
  }

}
