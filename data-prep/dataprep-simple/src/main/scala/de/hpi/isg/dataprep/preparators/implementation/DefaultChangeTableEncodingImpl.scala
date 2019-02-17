package de.hpi.isg.dataprep.preparators.implementation

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, Charset, CodingErrorAction}
import java.nio.file.{Files, Paths}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.EncodingNotDetectedException
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.mozilla.universalchardet.UniversalDetector

/**
  *
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

    val csvPath = preparator.getCsvPath.get  // TODO
    val loadEncoding = preparator.getCurrentEncoding
    val actualEncoding = detectEncoding(csvPath)
    val hasCorrectLoadEncoding = loadEncoding.isDefined && loadEncoding.get == actualEncoding

    if (!hasCorrectLoadEncoding) {
      dialect.setEncoding(actualEncoding)
      data = reloadWith(dialect, pipeline)
    }

    if (hasCorrectLoadEncoding || preparator.calApplicability(null, data, null) > 0) {
      val unmixedDialect = new EncodingUnmixer(csvPath).unmixEncoding(dialect)
      data = reloadWith(unmixedDialect, pipeline)
    }
    new ExecutionContext(data, errorAccumulator)
  }

  private def reloadWith(dialect: FileLoadDialect, pipeline: AbstractPipeline): DataFrame = {
    val createdDataset = new FlatFileDataLoader(dialect).load().getDataFrame
    pipeline.initMetadataRepository()
    createdDataset
  }

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
  private var csvFile: RandomAccessFile = _

  private case class DetectionUnit(startIndex: Int, endIndex: Int, encoding: String) {
    def length: Int = this.endIndex - this.startIndex
  }

  def unmixEncoding(dialect: FileLoadDialect): FileLoadDialect = {
    val path = "TODO"
    this.csvFile = new RandomAccessFile(this.csvPath, "r")

    val units = findUnits()
    val correctedUnits = correctUnits(units)
    writeCsv(correctedUnits, path)

    this.csvFile.close()

    dialect.setEncoding(WRITE_ENCODING)
    dialect.setUrl(path)
    dialect
  }

  private def findUnits(): Seq[DetectionUnit] = {
    this.csvFile.seek(0)
    val reader = new ByteLineReader(this.csvFile)
    val detector = new UniversalDetector(null)
    var units = Vector[DetectionUnit]()

    var unitStartIndex = 0
    while (!reader.fileEnd) {
      val unitEncoding = detectUnitEncoding(reader, detector)
      val unitEndIndex = reader.currentIndex
      units = units :+ DetectionUnit(unitStartIndex, unitEndIndex, unitEncoding)
      unitStartIndex = unitEndIndex
    }
    units
  }

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
  private def writeCsv(units: Seq[DetectionUnit], newPath: String): Unit = {
    val maxLength = units.maxBy(_.length).length
    val buf = new Array[Byte](maxLength)
    val newFile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newPath), WRITE_ENCODING))

    for (unit <- units) {
      this.csvFile.seek(unit.startIndex)
      this.csvFile.read(buf, 0, unit.length)
      val str = new String(buf, 0, unit.length, unit.encoding)
      newFile.write(str)
    }
    newFile.close()
  }

  private def detectUnitEncoding(reader: ByteLineReader, detector: UniversalDetector): String = {
    val MAX_ATTEMPTS = 50
    var attempts = 0
    detector.reset()

    do {
      var lineEnd = false
      do {
        lineEnd = reader.readLineBytes()
        detector.handleData(reader.buf, 0, reader.length)
      } while (!lineEnd)  // process data until we reach the line end
      reader.prepareForNextLine()
      attempts += 1

      // if we have tried MAX_ATTEMPTS lines without finding the encoding, or if the file has ended, pick the encoding with the highest confidence
      if (attempts > MAX_ATTEMPTS || reader.fileEnd) {
        detector.dataEnd()
      }

      // if even that didn't help, try turning the detector off and on again
      if (attempts > MAX_ATTEMPTS && detector.getDetectedCharset == null) {
        detector.reset()
        attempts = 0
      }
    } while (detector.getDetectedCharset == null && !reader.fileEnd)  // at the end of every line, check if done

    // detector bug: if it only read ASCII characters it doesn't detect any encoding
    // either that, or it wasn't able to detect the encoding, which we can't do anything about
    if (detector.getDetectedCharset == null) "ascii" else detector.getDetectedCharset
  }

  /**
    * Moves the boundary between two units to the exact point where the encoding changes
    * @return the corrected units
    */
  private def correctBoundary(first: DetectionUnit, second: DetectionUnit): (DetectionUnit, DetectionUnit) = {
    this.csvFile.seek(first.startIndex)
    val reader = new ByteLineReader(this.csvFile)
    var lineBytes = Vector[Byte]()
    var lineStartIndex = first.startIndex
    var validEncoding = true

    do {  // go through file until we find a line that can't be decoded with first.encoding
      lineStartIndex = first.startIndex + reader.currentIndex
      var lineEnd = false
      do {
        lineEnd = reader.readLineBytes()
        lineBytes = lineBytes ++ reader.buf.slice(0, reader.length)
      } while (!lineEnd)
      reader.prepareForNextLine()

      validEncoding = isValidEncodingFor(lineBytes.toArray, first.encoding)
    } while (validEncoding && reader.currentIndex < second.endIndex && !reader.fileEnd)

    val boundary = lineStartIndex  // this line could not be decoded => assume it is the boundary
    (DetectionUnit(first.startIndex, boundary, first.encoding), DetectionUnit(boundary, second.endIndex, second.encoding))
  }

  /**
    * Checks whether `bytes` can be decoded using `encoding`
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
  var length: Int = 0           // number of valid bytes in buf
  var currentIndex: Int = 0     // position in file, relative to the position where we started
  var fileEnd: Boolean = false  // true if currentIndex is at the file end
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
      // the rightmost part of this.buf can no longer be filled with new data
      // move lastValidIndex to the left to ensure we don't read stale data
      this.lastValidIndex -= this.buf.length - bytesRead - bufOffset
    }

    val lineEndIndex = this.buf.indexOf(NEWLINE_CHAR)
    var hasFoundLineEnd = lineEndIndex != -1 && lineEndIndex < lastValidIndex
    this.length = if (hasFoundLineEnd) lineEndIndex + 1 else lastValidIndex
    this.currentIndex += this.length

    if (hasReachedFileEnd && this.length == lastValidIndex) {
      this.fileEnd = true
      hasFoundLineEnd = true
    }

    hasFoundLineEnd
  }

  private def hasReachedFileEnd = this.lastValidIndex < this.buf.length

  /**
    * Moves bytes from the next line to the beginning of buf, make sure they don't get overwritten by next read
    */
  def prepareForNextLine(): Unit = {
    System.arraycopy(this.buf, this.length, this.buf, 0, this.buf.length - this.length)
  }
}
