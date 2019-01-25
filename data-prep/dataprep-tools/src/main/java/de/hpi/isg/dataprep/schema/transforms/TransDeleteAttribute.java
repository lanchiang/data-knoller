package de.hpi.isg.dataprep.schema.transforms;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.SimpleSchemaMapping;

/**
 * @author lan.jiang
 * @since 1/25/19
 */
public class TransDeleteAttribute extends Transform {

    private Attribute sourceAttribute;
    private Attribute targetAttribute;

    public TransDeleteAttribute(Attribute sourceAttribute) {
        this.sourceAttribute = sourceAttribute;
        this.targetAttribute = null;
    }

    @Override
    public void reformSchema(SchemaMapping schemaMapping) {
        SimpleSchemaMapping simpleSchemaMapping = (SimpleSchemaMapping) schemaMapping;
        if (!simpleSchemaMapping.getCurrentSchema().attributeExist(sourceAttribute)) {
            throw new RuntimeException("Attribute does not exist.");
        }
        simpleSchemaMapping.updateMapping(sourceAttribute, targetAttribute);
        for (Attribute attribute : simpleSchemaMapping.getCurrentSchema().getAttributes()) {
            if (!attribute.equals(sourceAttribute)) {
                simpleSchemaMapping.updateMapping(attribute, attribute);
            }
        }
        simpleSchemaMapping.finalizeUpdate();
    }
}
