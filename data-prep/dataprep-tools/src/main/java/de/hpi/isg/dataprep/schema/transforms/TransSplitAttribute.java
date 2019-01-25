package de.hpi.isg.dataprep.schema.transforms;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;

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
    public void reformSchema(SchemaMapping schemaMapping) {
        // check whether the sourceAttribute exists in the schema
        if (!schemaMapping.getCurrentSchema().attributeExist(sourceAttribute)) {
            throw new RuntimeException("Attribute does not exist.");
        }

//        if (SchemaUtils.targetAttributeExist(schemaMapping.getCurrentSchema(), targetAttributes)!=null) {
//            throw new RuntimeException("Attribute(s) already exist in the schema.");
//        }

        Schema currentSchema = schemaMapping.getCurrentSchema();
        // here ready to execute this transform
        for (Attribute targetAttribute : targetAttributes) {
            schemaMapping.updateMapping(sourceAttribute, targetAttribute);
        }
        for (Attribute attribute : currentSchema.getAttributes()) {
            schemaMapping.updateMapping(attribute, attribute);
        }
        schemaMapping.finalizeUpdate();
//        schemaMapping.updateSchemaMappingNodes();
//        schemaMapping.updateSchema();
    }
}
