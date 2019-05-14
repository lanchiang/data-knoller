package de.hpi.isg.dataprep.preparators.implementation

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, Charset, CodingErrorAction}
import java.nio.file.{Files, Paths}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.EncodingNotDetectedException
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.mozilla.universalchardet.UniversalDetector

/**
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class DefaultChangeTableEncodingImpl extends AbstractPreparatorImpl {
  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeTableEncoding]
    val pipeline = preparator.getPreparation.getPipeline
    var data = dataFrame
    val dialect = pipeline.getDialect


    /**
      * compare the encoding in the metadata with the detected one. On mismatch reload the data with the correct encoding
      * @return a new ExecutionContext with the correct encoded data
      */
    val csvPath = preparator.getCsvPath.get
    val loadEncoding = preparator.getCurrentEncoding
    val actualEncoding = detectEncoding(csvPath)
    val hasCorrectLoadEncoding = loadEncoding.isDefined && loadEncoding.get == actualEncoding

    if (!hasCorrectLoadEncoding) {
      dialect.setEncoding(actualEncoding)
      data = reloadWith(dialect, pipeline)
    }

    // if we still have errors despite using the correct encoding, the file probably contains multiple encodings
    if (hasCorrectLoadEncoding || preparator.calApplicability(null, data, null, null) > 0) {
      val unmixedDialect = new EncodingUnmixer(csvPath).unmixEncoding(dialect)
      data = reloadWith(unmixedDialect, pipeline)
    }
    new ExecutionContext(data, errorAccumulator)
  }

  /**
    * reload the data with the specified encoding and initialise  the metadataRepository new
    * @param dialect  the dialect with which the data should be reloaded
    * @param pipeline the actual pipeline for which the metadataRepository has to be reinitialized
    * @return a dataframe with the correct encoding
    */
  private def reloadWith(dialect: FileLoadDialect, pipeline: AbstractPipeline): DataFrame = {
    val createdDataset = new FlatFileDataLoader(dialect).load().getDataFrame
    pipeline.getMetadataRepository.clear()
    pipeline.initMetadataRepository()
    createdDataset
  }

  /**
    * detect the encoding of a csv
    * @param csvPath the path to the file on which the encoding should be detected
    * @return the detected encoding as a string
    */
  private def detectEncoding(csvPath: String): String = {
    val inStream = Files.newInputStream(Paths.get(csvPath))

    val buf = new Array[Byte](4096)
    val detector = new UniversalDetector(null)

    var bytesRead = inStream.read(buf)
    while (bytesRead > 0 && !detector.isDone) {
      detector.handleData(buf, 0, bytesRead)
      bytesRead = inStream.read(buf)
    }
    detector.dataEnd()

    val encoding = detector.getDetectedCharset
    if (encoding == null) throw new EncodingNotDetectedException(csvPath)
    encoding
  }

}

private class EncodingUnmixer(csvPath: String) {
  val WRITE_ENCODING = "UTF-8"
  private var mixedFile: RandomAccessFile = _

  /**
    * Range of bytes for which an encoding was detected
    * @param startPos position of the first byte (inclusive)
    * @param endPos   position of the last byte (exclusive)
    * @param encoding the encoding that was detected
    */
  private case class DetectionUnit(startPos: Int, endPos: Int, encoding: String) {
    def length: Int = this.endPos - this.startPos
  }

  /**
    * Converts a file with multiple encodings into a consistently encoded file
    * @param dialect the dialect with which the original file was loaded
    * @return a dialect pointing to the new file and with the correct encoding
    */
  def unmixEncoding(dialect: FileLoadDialect): FileLoadDialect = {
    val path = generateNewPath(dialect.getUrl)
    this.mixedFile = new RandomAccessFile(this.csvPath, "r")

    val units = findUnits()
    val correctedUnits = correctUnits(units)
    writeUnmixedFile(correctedUnits, path)

    this.mixedFile.close()

    dialect.setEncoding(WRITE_ENCODING)
    dialect.setUrl(path)
    dialect
  }

  private def generateNewPath(str: String): String = {
    val oldPath = Paths.get(str)
    val filename = System.currentTimeMillis.toString + "_unmixed_" + oldPath.getFileName
    Paths.get(oldPath.getParent.toString, filename).toString
  }

  /**
    * Segments the file into DetectionUnits and detects their encoding
    */
  private def findUnits(): Seq[DetectionUnit] = {
    this.mixedFile.seek(0)
    val reader = new ByteLineReader(this.mixedFile)
    val detector = new UniversalDetector(null)
    var units = Vector[DetectionUnit]()

    var unitStartPos = 0
    while (!reader.fileEnd) {
      val unitEncoding = detectUnitEncoding(reader, detector)
      val unitEndPos = reader.currentPos
      units = units :+ DetectionUnit(unitStartPos, unitEndPos, unitEncoding)
      unitStartPos = unitEndPos
    }
    units
  }

  /**
    * The boundaries of the different encodings might not exactly coincide with the unit boundaries. Try fixing this
    */
  private def correctUnits(units: Seq[DetectionUnit]): Seq[DetectionUnit] = {
    var previousUnit = units.head
    var correctedUnits = Vector[DetectionUnit](units.head)

    for (unit <- units.drop(1)) {
      if (unit.encoding != previousUnit.encoding) {
        val correctedBoundaryUnits = correctBoundary(previousUnit, unit)
        correctedUnits = correctedUnits.dropRight(1) :+ correctedBoundaryUnits._1 :+ correctedBoundaryUnits._2
      } else {
        correctedUnits = correctedUnits :+ unit
      }
      previousUnit = unit
    }
    correctedUnits
  }

  /**
    * Writes the content of this.csvFile into a new CSV file using a consistent encoding
    */
  private def writeUnmixedFile(units: Seq[DetectionUnit], newPath: String): Unit = {
    val maxLength = units.maxBy(_.length).length
    val buf = new Array[Byte](maxLength)
    val newFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newPath), WRITE_ENCODING))

    for (unit <- units) {
      this.mixedFile.seek(unit.startPos)
      this.mixedFile.read(buf, 0, unit.length)
      val str = new String(buf, 0, unit.length, unit.encoding)
      newFile.write(str)
    }
    newFile.close()
  }

  /**
    * Reads the next unit from the file and detects its encoding
    */
  private def detectUnitEncoding(reader: ByteLineReader, detector: UniversalDetector): String = {
    val MAX_LINES = 50  // not a hard maximum, a unit can grow larger than 50 lines
    var linesInUnit = 0
    detector.reset()

    do {
      var lineEnd = false
      do {
        lineEnd = reader.readLineBytes()
        detector.handleData(reader.buf, 0, reader.length)
      } while (!lineEnd)  // process data until we reach the line end
      linesInUnit += 1
      reader.prepareForNextLine()

      // if we have tried MAX_LINES lines without finding the encoding, or if the file has ended, pick the encoding with the highest confidence
      if (linesInUnit > MAX_LINES || reader.fileEnd) {
        detector.dataEnd()
      }

      // if even that didn't help, try turning the detector off and on again
      if (linesInUnit > MAX_LINES && detector.getDetectedCharset == null) {
        detector.reset()
        linesInUnit = 0
      }
    } while (detector.getDetectedCharset == null && !reader.fileEnd)  // at the end of every line, check if done

    // detector bug: if it only read ASCII characters, it doesn't detect any encoding
    if (detector.getDetectedCharset == null) "ascii" else detector.getDetectedCharset
  }

  /**
    * Moves the boundary between two units to the exact point where the encoding changes
    * @return the corrected units
    */
  private def correctBoundary(first: DetectionUnit, second: DetectionUnit): (DetectionUnit, DetectionUnit) = {
    this.mixedFile.seek(first.startPos)
    val reader = new ByteLineReader(this.mixedFile)
    var lineBytes = Vector[Byte]()
    var lineStartPos = first.startPos
    var validEncoding = true

    do {  // go through file until we find a line that can't be decoded with first.encoding
      lineStartPos = first.startPos + reader.currentPos
      var lineEnd = false
      do {
        lineEnd = reader.readLineBytes()
        lineBytes = lineBytes ++ reader.buf.slice(0, reader.length)
      } while (!lineEnd)
      reader.prepareForNextLine()

      validEncoding = isValidEncodingFor(lineBytes.toArray, first.encoding)
    } while (validEncoding && reader.currentPos < second.endPos && !reader.fileEnd)

    val boundary = lineStartPos  // this line could not be decoded => assume it is the boundary
    (DetectionUnit(first.startPos, boundary, first.encoding), DetectionUnit(boundary, second.endPos, second.encoding))
  }

  /**
    * Checks whether a byte array can be decoded using an encoding
    */
  private def isValidEncodingFor(bytes: Array[Byte], encoding: String): Boolean = {
    val buf = ByteBuffer.wrap(bytes)
    val decoder = Charset.forName(encoding).newDecoder()
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(buf)
    } catch {
      case _: CharacterCodingException => return false
    }
    true
  }
}

private class ByteLineReader(csvFile: RandomAccessFile) {
  private val NEWLINE_CHAR = 0x0A  // ASCII byte for newline (most encodings are backwards compatible with ASCII)
  private val BUFFER_SIZE = 4096

  val buf = new Array[Byte](BUFFER_SIZE)
  var length: Int = 0            // number of valid bytes in buf
  var currentPos: Int = 0        // position in file, relative to the position where we started
  var fileEnd: Boolean = false   // true if currentPos is at the file end
  private var lastValidIndex = this.buf.length  // marks the last valid byte in buf

  /**
    * Reads bytes into the buffer until the buffer is full or a line end is found
    * @return true if a line (or file) end was found
    */
  def readLineBytes(): Boolean = {
    // make sure we don't overwrite bytes whose line end hasn't been found yet
    val bufOffset = if (this.length == 0) 0 else this.buf.length - this.length
    var bytesRead = this.csvFile.read(this.buf, bufOffset, this.buf.length - bufOffset)
    if (bytesRead == -1) bytesRead = 0  // handle file end

    if (bytesRead + bufOffset < this.buf.length) {  // we have reached the end of the file
      // the rightmost part of buf can no longer be filled with new data
      // move lastValidIndex to the left to ensure we don't read stale data
      this.lastValidIndex -= this.buf.length - bytesRead - bufOffset
    }

    val lineEndIndex = this.buf.indexOf(NEWLINE_CHAR)
    var hasFoundLineEnd = lineEndIndex != -1 && lineEndIndex < lastValidIndex
    this.length = if (hasFoundLineEnd) lineEndIndex + 1 else lastValidIndex
    this.currentPos += this.length

    if (hasReachedFileEnd && this.length == lastValidIndex) {
      this.fileEnd = true
      hasFoundLineEnd = true
    }

    hasFoundLineEnd
  }

  private def hasReachedFileEnd = this.lastValidIndex < this.buf.length

  /**
    * Moves bytes from the next line to the beginning of buf, so they don't get overwritten by the next read
    */
  def prepareForNextLine(): Unit = {
    System.arraycopy(this.buf, this.length, this.buf, 0, this.buf.length - this.length)
  }
}
