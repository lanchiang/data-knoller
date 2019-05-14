package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.SuggestableRemovePreamble.HISTOGRAM_ALGORITHM
import de.hpi.isg.dataprep.preparators.define.SuggestableRemovePreamble.HISTOGRAM_ALGORITHM.HISTOGRAM_ALGORITHM
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2019-04-08
  */
class SuggestableRemovePreamble(var boundary: String) extends AbstractPreparator {

  def this() { this(null) }

  preparatorTarget = PreparatorTarget.TABLE_BASED

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    val localContext = dataset.sqlContext.sparkSession
    import localContext.implicits._

    // remove empty rows
    val withoutEmptyLineDF = dataset.na.drop("all")

    // aggregate value length histogram of each row as an array
//    val histograms = withoutEmptyLineDF.map(row => SuggestableRemovePreamble.valueLengthHistogram(row)).collect()
    val histograms = dataset.map(row => SuggestableRemovePreamble.valueLengthHistogram(row)).collect()

    var lastNonempty = Seq.empty[Double]
    var nonEmptyHistograms = Array.empty[Seq[Double]]
    for (histogram <- histograms) {
      if (histogram.exists(_ != 0)) {
        lastNonempty = histogram
      }
      nonEmptyHistograms = nonEmptyHistograms :+ lastNonempty
    }

    // calculate the histogram difference of the neighbouring pairs of histograms, zipping them with index
    val histogramDifference = nonEmptyHistograms
            .sliding(2)
            .map(pair => SuggestableRemovePreamble.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Bhattacharyya))
            .toSeq
    val (min, max) = (histogramDifference.min, histogramDifference.max)
    val histogramDiff = histogramDifference
//            .map(diff => (diff-min)/(max-min))
            .zipWithIndex
            .map(pair => {
              val x = (pair._2+1).toString.concat("||").concat((pair._2+2).toString)
              val y = pair._1
              (x,y)
            }).sortBy(indexedHistDiff => indexedHistDiff._2)(Ordering[Double].reverse)

    // Gets score 1 if the cliff is larger than the threshold.
    val histDiffCliff = histogramDiff
            .sliding(2)
            .map(pair => (pair(0)._1 , pair(0)._2 - pair(1)._2))
            .maxBy(drop => drop._2)
    histDiffCliff._2 >= SuggestableRemovePreamble.hasPreamble_threshold match {
      case true => {
        boundary = histDiffCliff._1
        1f
      }
      case false => 0f
    }

//    val numAboveThreshold = histogramDiff.unzip._2.count(hasPreambleScore => hasPreambleScore >= SuggestRemovePreamble.hasPreamble_threshold)
//    // if a preamble is detected, return 1, as otherwise return 0
//    // Todo: slack the score by considering how different the peak is from others
//    numAboveThreshold match {
//      case count if count > 0 => 1f // preamble detected
//      case _ => 0f // no preamble detected
//    }
  }

  override def getAffectedProperties: util.List[String] = {
    List.empty[String].asJava
  }

  override def toString = s"SuggestableRemovePreamble($boundary)"
}

object SuggestableRemovePreamble {

//  val hasPreamble_threshold = 0.7 // the threshold to decide whether there is any preamble in the file
  val hasPreamble_threshold = 0.4 // the threshold to decide whether there is any preamble in the file. We deem it yes if the cliff is larger than the threshold

  /**
    * return a histogram of value length of each field in the row.
    *
    * @param row is the row whose value length in the respective field will be calculated.
    * @return a histogram of value length of each field in the row.
    */
  def valueLengthHistogram(row: Row) : Seq[Double] = {
    val oriArr = row.getValuesMap[Any](row.schema.fieldNames)
            .map(keyVal => Try{keyVal._2.toString} match {
              case Success(value) => value.length.toDouble
              case Failure(exception) => 0
            })
            .toSeq
    val sum = oriArr.sum
    oriArr
  }

  /**
    * Calculates the histogram difference between the two given histograms represented as two double sequences.
    *
    * @param histogram1 is the first histogram
    * @param histogram2 is the second histogram
    * @param algorithm is the algorithm used to calculate the histogram difference.
    * @return the histogram difference as a double
    */
  def histogramDifference(histogram1: Seq[Double], histogram2: Seq[Double], algorithm: HISTOGRAM_ALGORITHM): Double = {
    val ave_hist1 = histogram1.sum / histogram1.size
    val ave_hist2 = histogram2.sum / histogram2.size

    val bhattacharyya_factor = 1 / Math.sqrt(ave_hist1*ave_hist2*histogram1.size*histogram2.size)

    var result = histogram1.zip(histogram2).map{
      case (x,y) => algorithm match {
        case HISTOGRAM_ALGORITHM.Min => Math.min(x,y)
        case HISTOGRAM_ALGORITHM.Correlation => {
          (x-ave_hist1)*(y-ave_hist2)
        }
        case HISTOGRAM_ALGORITHM.Chi_square => {
          x match {
            case 0.0 => 0.0
            case _ => Math.pow((x-y), 2)/x
          }
        }// asymmetric to (hist1, hist2) and (hist2, hist1)
        case HISTOGRAM_ALGORITHM.Bhattacharyya => Math.sqrt(x*y)
      }
    }.sum

    algorithm match {
      case HISTOGRAM_ALGORITHM.Bhattacharyya => {
        1 - bhattacharyya_factor*result
      }
      case _ => result
    }
  }

  object HISTOGRAM_ALGORITHM extends Enumeration {
    type HISTOGRAM_ALGORITHM = Value
    val Min, Correlation, Chi_square, Bhattacharyya = Value
  }
}