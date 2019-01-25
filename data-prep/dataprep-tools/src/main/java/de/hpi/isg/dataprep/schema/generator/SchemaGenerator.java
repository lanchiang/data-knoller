package de.hpi.isg.dataprep.schema.generator;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.SimpleSchemaMapping;

import java.util.List;

/**
 * The schema generator accepts source schema and a list of transformations as the input, and produces a target schema as the output.
 *
 * @author lan.jiang
 * @since 1/24/19
 */
public class SchemaGenerator {

    private Schema sourceSchema;
    private Schema targetSchema;
    private List<Transform> transforms;

    private SchemaMapping schemaMapping;

    public SchemaGenerator(Schema sourceSchema, List<Transform> transforms) {
        this.sourceSchema = sourceSchema;
        this.transforms = transforms;

        schemaMapping = new SimpleSchemaMapping(this.sourceSchema);
    }

    public void constructTargetSchema() {
        for (Transform transform : transforms) {
            transform.reformSchema(schemaMapping);
        }
        targetSchema = schemaMapping.getCurrentSchema();
    }

    public SchemaMapping createSchemaMapping() {
        return schemaMapping.createSchemaMapping();
    }
}
