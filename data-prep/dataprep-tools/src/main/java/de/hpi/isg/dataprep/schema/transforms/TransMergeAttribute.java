package de.hpi.isg.dataprep.schema.transforms;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.SimpleSchemaMapping;

import java.util.Arrays;
import java.util.Optional;

/**
 * @author lan.jiang
 * @since 1/22/19
 */
public class TransMergeAttribute extends Transform {

    private Attribute[] sourceAttributes;
    private Attribute targetAttribute;

    public TransMergeAttribute(Attribute[] sourceAttributes, Attribute targetAttribute) {
        this.sourceAttributes = sourceAttributes;
        this.targetAttribute = targetAttribute;
    }

    @Override
    public void reformSchema(SchemaMapping schemaMapping) {
        SimpleSchemaMapping simpleSchemaMapping = (SimpleSchemaMapping) schemaMapping;
        Schema currentSchema = simpleSchemaMapping.getCurrentSchema();

        // check whether the source attributes exist in the schema
        Optional<Attribute> hasNonExist =  Arrays.stream(sourceAttributes)
                .filter(attribute -> !simpleSchemaMapping.getCurrentSchema().attributeExist(attribute))
                .findFirst();
        if (hasNonExist.isPresent()) {
            throw new RuntimeException("Some source attributes do not exist.");
        }
//        if (SchemaUtils.targetAttributeExist(currentSchema, targetAttribute)!=null) {
//            throw new RuntimeException("Target attribute already exist in the schema.");
//        }

        // here ready to execute this transformation
        for (Attribute attribute : sourceAttributes) {
            simpleSchemaMapping.updateMapping(attribute, targetAttribute);
        }
        for (Attribute attribute : currentSchema.getAttributes()) {
//            System.out.println(attribute.getName());
            simpleSchemaMapping.updateMapping(attribute, attribute);
        }
        simpleSchemaMapping.finalizeUpdate();
//        schemaMapping.updateSchemaMappingNodes();
//        schemaMapping.updateSchema();
    }
}
