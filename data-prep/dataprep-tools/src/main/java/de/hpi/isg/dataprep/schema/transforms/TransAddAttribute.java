package de.hpi.isg.dataprep.schema.transforms;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.SimpleSchemaMapping;

/**
 * @author lan.jiang
 * @since 1/25/19
 */
public class TransAddAttribute extends Transform {

    private Attribute sourceAttribute;
    private Attribute targetAttribute;

    public TransAddAttribute(Attribute targetAttribute) {
        this.targetAttribute = targetAttribute;
        this.sourceAttribute = null;
    }

    @Override
    public void buildStep(SchemaMapping schemaMapping) {
        SimpleSchemaMapping simpleSchemaMapping = (SimpleSchemaMapping) schemaMapping;
        if (this.sourceAttribute != null) {
            throw new RuntimeException(new IllegalArgumentException("Unexpected value in field: sourceAttribute"));
        }
        Schema currentSchema = simpleSchemaMapping.getCurrentSchema();
        if (currentSchema.attributeExist(targetAttribute)) {
            throw new RuntimeException("Attribute already exists.");
        }

        simpleSchemaMapping.updateMapping(sourceAttribute, targetAttribute);
        for (Attribute attribute : currentSchema.getAttributes()) {
            if (!attribute.equals(targetAttribute)) {
                simpleSchemaMapping.updateMapping(attribute, attribute);
            }
        }
        simpleSchemaMapping.finalizeUpdate();
    }
}
