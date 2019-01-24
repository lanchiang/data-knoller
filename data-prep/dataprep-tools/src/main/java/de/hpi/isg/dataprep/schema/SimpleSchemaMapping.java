package de.hpi.isg.dataprep.schema;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represents the schema mapping from the current schema to the target schema. It is used by
 * the decision engine to calculate the applicability score
 * for preparator suggestion.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class SimpleSchemaMapping implements SchemaMapping {

    private Schema sourceSchema;
    private Schema targetSchema;

    private Schema currentSchema;

    /**
     * Each key represents a particular attribute in the source schema, its value is a set of attributes
     * in the target schema that are derived from the attribute in the key.
     */
    private Map<Attribute, Set<Attribute>> mapping;

    public SimpleSchemaMapping(Schema sourceSchema, Schema targetSchema,
                               Map<Attribute, Set<Attribute>> mapping) {
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.mapping = mapping;

        this.currentSchema = this.sourceSchema;
    }

    public SimpleSchemaMapping(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
//        this.targetSchema = new Schema(null);

        this.mapping = new HashMap<>();
        for (Attribute attribute : sourceSchema.getAttributes()) {
            this.mapping.putIfAbsent(attribute, new HashSet<>());
        }

        this.currentSchema = this.sourceSchema;
    }

    public Schema getSourceSchema() {
        return sourceSchema;
    }

    public Schema getTargetSchema() {
        return targetSchema;
    }

    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }

    public Map<Attribute, Set<Attribute>> getMapping() {
        return mapping;
    }

    /**
     * Get the set of attributes in the target schema that are derived from the given {@link Attribute}.
     *
     * @param attribute in the source schema
     * @return the set of attributes in the target schema that are derived from the given attribute. If
     * the given attribute does not exist in the source schema, return null.
     */
    public Set<Attribute> getTargetBySourceAttribute(Attribute attribute) {
        return mapping.getOrDefault(attribute, null);
    }

    @Override
    public Set<Attribute> getTargetBySourceAttributeName(String attributeName) {
        Optional<Set<Attribute>> oTargetAttributes = mapping.entrySet().stream()
                .filter(attrMapping -> attrMapping.getKey().getName().equals(attributeName))
                .map(attrMapping -> attrMapping.getValue())
                .findFirst();
        return oTargetAttributes.orElse(null);
    }

    @Override
    public Set<Attribute> getSourceByTargetAttribute(Attribute attribute) {
        Set<Attribute> sourceAttributes = mapping.entrySet().stream()
                .filter(attrMapping -> attrMapping.getValue().contains(attribute))
                .map(attrMapping -> attrMapping.getKey())
                .collect(Collectors.toSet());
        return sourceAttributes.size()==0?null:sourceAttributes;
    }

    public Set<Attribute> getSourceByTargetAttributeName(String attributeName) {
        Set<Attribute> sourceAttributes = mapping.entrySet().stream()
                .filter(attrMapping -> {
                    long countAttr = attrMapping.getValue().stream()
                            .filter(attribute -> attribute.getName().equals(attributeName)).count();
                    return countAttr>0?true:false;
                })
                .map(attrMapping -> attrMapping.getKey())
                .collect(Collectors.toSet());
        return sourceAttributes.size()==0?null:sourceAttributes;
    }

    @Override
    public void constructSchemaMapping(List<Transform> transforms) {
        for (Transform transform : transforms) {
            transform.reformSchema(this);
        }
    }

    @Override
    public void updateMapping(Attribute sourceAttribute, Attribute targetAttribute) {
        if (sourceAttribute == null) {
            throw new RuntimeException("Source attribute can not be found in the current schema.");
        }
        if (targetAttribute != null) {
            this.mapping.putIfAbsent(sourceAttribute, new HashSet<>());
            this.mapping.get(sourceAttribute).add(targetAttribute);
            this.mapping.putIfAbsent(targetAttribute, new HashSet<>());
//        } else {
//            this.mapping.get(sourceAttribute).remove(targetAttribute);
        }
    }

    @Override
    public void updateSchema(Schema latestSchema) {
        this.currentSchema = latestSchema;
    }
}
