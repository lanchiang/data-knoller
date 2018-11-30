package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.model.repository.ErrorRepository
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog
import de.hpi.isg.dataprep.preparators.define.AddProperty
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, Metadata}

import scala.collection.JavaConverters._

class ChangeDateFormatTest extends PreparatorScalaTest {
    "Test" should "do something" in {
        val preparator = new AddProperty("classic", DataType.PropertyType.INTEGER, 4, 8)

        val preparation = new Preparation(preparator)
        pipeline.addPreparation(preparation)
        pipeline.executePipeline()

        var trueErrorLog = List.empty[ErrorLog]
        val trueErrorRepository = new ErrorRepository(trueErrorLog.asJava)

        // First test error log repository
        trueErrorRepository shouldEqual pipeline.getErrorRepository

        val updated = pipeline.getRawData
        val updatedSchema = updated.schema

        val trueSchema = StructType(List(
            StructField("id", DataTypes.StringType, true, Metadata.empty),
            StructField("identifier", DataTypes.StringType, true, Metadata.empty),
            StructField("species_id", DataTypes.IntegerType, true, Metadata.empty),
            StructField("height", DataTypes.IntegerType, true, Metadata.empty),
            StructField("classic", DataTypes.IntegerType, false, Metadata.empty),
            StructField("weight", DataTypes.IntegerType, true, Metadata.empty),
            StructField("base_experience", DataTypes.StringType, true, Metadata.empty),
            StructField("order", DataTypes.IntegerType, true, Metadata.empty),
            StructField("is_default", DataTypes.IntegerType, true, Metadata.empty),
            StructField("date", DataTypes.StringType, true, Metadata.empty)
        ))

        // Second test whether the schema is correctly updated.
        trueSchema shouldEqual updatedSchema
    }
}
