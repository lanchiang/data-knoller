package de.hpi.isg.dataprep.utils;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * This is the class to provide update related utility functions.
 *
 * @author lan.jiang
 * @since 1/25/19
 */
@Deprecated
public class UpdateUtils {

    /**
     * Update the schema mapping with the given preparator.
     *
     * @param schemaMapping
     * @param executionContext
     */
    public static void updateSchemaMapping(SchemaMapping schemaMapping, ExecutionContext executionContext) {
        Dataset<Row> updatedDataset = executionContext.newDataFrame();

        Schema latest = new Schema(updatedDataset.schema());
        schemaMapping.setCurrentSchema(latest);
    }

    public static void updateMetadata(Pipeline pipeline, AbstractPreparator preparator) {
        pipeline.updateTargetMetadata(preparator.getUpdateMetadata());
    }
}
