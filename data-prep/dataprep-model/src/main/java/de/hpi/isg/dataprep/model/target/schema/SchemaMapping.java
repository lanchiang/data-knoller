package de.hpi.isg.dataprep.model.target.schema;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents the schema mapping from the current schema to the target schema. It is used by
 * the {@link de.hpi.isg.dataprep.model.target.system.DecisionEngine} to calculate the applicability score
 * for preparator suggestion.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class SchemaMapping {

    private Schema sourceSchema;
    private Schema targetSchema;

    /**
     * Each key represents a particular attribute in the source schema, its value is a set of attributes
     * in the target schema that are derived from the attribute in the key.
     */
    private Map<Attribute, Set<Attribute>> mapping;

    public SchemaMapping(Schema sourceSchema, Schema targetSchema,
                         Map<Attribute, Set<Attribute>> mapping) {
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.mapping = mapping;
    }

    public Schema getSourceSchema() {
        return sourceSchema;
    }

    public Schema getTargetSchema() {
        return targetSchema;
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

    public Set<Attribute> getTargetBySourceAttributeName(String attributeName) {
        Optional<Set<Attribute>> oTargetAttributes = mapping.entrySet().stream()
                .filter(attrMapping -> attrMapping.getKey().getName().equals(attributeName))
                .map(attrMapping -> attrMapping.getValue())
                .findFirst();
        return oTargetAttributes.orElse(null);
    }

    /**
     * Get the set of attributes in the source schema that derive the given target {@link Attribute}.
     *
     * @param attribute in the target schema
     * @return the set of attributes in the source schema that derive the given target attribute. If the
     * given attribute is not derived from any attribute in the source schema, return null.
     */
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
}
