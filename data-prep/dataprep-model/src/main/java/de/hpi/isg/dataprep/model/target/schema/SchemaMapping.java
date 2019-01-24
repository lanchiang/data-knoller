package de.hpi.isg.dataprep.model.target.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface provides the methods that a concrete schema mapping class must implement. It is used by the decision engine
 * to calculate the applicability score for preparator suggestion.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public interface SchemaMapping {

    Schema getSourceSchema();

    Schema getTargetSchema();

    Schema getCurrentSchema();

    Map<Attribute, Set<Attribute>> getMapping();

    /**
     * Get the set of attributes in the target schema that are derived from the given {@link Attribute}.
     *
     * @param attribute in the source schema
     * @return the set of attributes in the target schema that are derived from the given attribute. If
     * the given attribute does not exist in the source schema, return null.
     */
    Set<Attribute> getTargetBySourceAttribute(Attribute attribute);

    Set<Attribute> getTargetBySourceAttributeName(String attributeName);

    /**
     * Get the set of attributes in the source schema that derive the given target {@link Attribute}.
     *
     * @param attribute in the target schema
     * @return the set of attributes in the source schema that derive the given target attribute. If the
     * given attribute is not derived from any attribute in the source schema, return null.
     */
    Set<Attribute> getSourceByTargetAttribute(Attribute attribute);

    Set<Attribute> getSourceByTargetAttributeName(String attributeName);

    /**
     * Construct the mapping of each attributes in this schema with the given ordered list of transforms.
     *
     * @param transforms the list of transformations used to construct the mapping.
     */
    void constructSchemaMapping(List<Transform> transforms);

    void updateMapping(Attribute sourceAttribute, Attribute targetAttribute);

    void updateSchema(Schema latestSchema);
}
