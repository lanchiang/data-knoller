package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.DialectBuilder
import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.load.{FlatFileDataLoader, SparkDataLoader}
import de.hpi.isg.dataprep.metadata.{PreambleExistence, PropertyDataType, PropertyExistence}
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Attribute, Transform}
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute
import de.hpi.isg.dataprep.util.DataType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

trait DataLoadingConfig extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  protected var pipeline: AbstractPipeline = _
  protected var dataContext: DataContext = _
  protected var dialect: FileLoadDialect = _
  private var transforms: List[Transform] = _

  protected var emptyMetadata: types.Metadata = org.apache.spark.sql.types.Metadata.empty


  override protected def beforeEach(): Unit = {
    pipeline = new Pipeline(dataContext)
  }

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    transforms = createTransformsManually
    // generate target metadata
    val targetMetadata = createTargetMetadataManually
    val path = File("src") / "test" / "resources" / "pokemon.csv" toString()
    dialect = new DialectBuilder().hasHeader(true).inferSchema(true).url(path).buildDialect
    //        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, schemaMapping);
    val dataLoader: SparkDataLoader = new FlatFileDataLoader(dialect, targetMetadata.asJava, transforms.asJava)
    dataContext = dataLoader.load
  }


  private def createTargetMetadataManually: Set[Metadata] = {
    val targetMetadata = new ListBuffer[Metadata]
    targetMetadata += new PreambleExistence(false)
    targetMetadata += new PropertyExistence("month", true)
    targetMetadata += new PropertyExistence("day", true)
    targetMetadata += new PropertyExistence("year", true)
    targetMetadata += new PropertyDataType("month", DataType.PropertyType.STRING)
    targetMetadata += new PropertyDataType("day", DataType.PropertyType.STRING)
    targetMetadata += new PropertyDataType("year", DataType.PropertyType.STRING)
    targetMetadata.toSet
  }

  private def createTargetMetadataFromFile: Set[Metadata] = {
    val targetMetadata: Set[Metadata] = Set[Metadata]()
    targetMetadata
  }

  private def createRandomTransforms: List[Transform] = {
    val transforms: List[Transform] = List[Transform]()
    transforms
  }

  private def createTransformsManually: List[Transform] = { // generate schema mapping
    val transforms = new ListBuffer[Transform]

    val sourceAttribute = new Attribute(StructField("date", DataTypes.StringType, nullable = true, emptyMetadata))
    val deleteAttr = new TransDeleteAttribute(sourceAttribute)
    transforms += deleteAttr
    transforms.toList
  }
}
