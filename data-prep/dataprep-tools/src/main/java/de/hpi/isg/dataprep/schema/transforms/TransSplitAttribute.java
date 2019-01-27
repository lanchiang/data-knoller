package de.hpi.isg.dataprep.schema.transforms;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.SimpleSchemaMapping;

/**
 * @author lan.jiang
 * @since 1/22/19
 */
public class TransSplitAttribute extends Transform {

    private Attribute sourceAttribute;
    private Attribute[] targetAttributes;

    public TransSplitAttribute(Attribute sourceAttribute, Attribute[] targetAttributes) {
        this.sourceAttribute = sourceAttribute;
        this.targetAttributes = targetAttributes;
    }

    @Override
    public void buildStep(SchemaMapping schemaMapping) {
        SimpleSchemaMapping simpleSchemaMapping = (SimpleSchemaMapping) schemaMapping;
        // check whether the sourceAttribute exists in the schema
        if (!simpleSchemaMapping.getCurrentSchema().attributeExist(sourceAttribute)) {
            throw new RuntimeException("Attribute does not exist.");
        }

//        if (SchemaUtils.targetAttributeExist(schemaMapping.getCurrentSchema(), targetAttributes)!=null) {
//            throw new RuntimeException("Attribute(s) already exist in the schema.");
//        }

        Schema currentSchema = simpleSchemaMapping.getCurrentSchema();
        // here ready to execute this transform
        for (Attribute targetAttribute : targetAttributes) {
            simpleSchemaMapping.updateMapping(sourceAttribute, targetAttribute);
        }
        for (Attribute attribute : currentSchema.getAttributes()) {
            simpleSchemaMapping.updateMapping(attribute, attribute);
        }
        simpleSchemaMapping.finalizeUpdate();
    }
}
