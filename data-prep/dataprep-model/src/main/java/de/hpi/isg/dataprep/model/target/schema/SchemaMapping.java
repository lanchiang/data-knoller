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

    Schema getCurrentSchema();

    Schema getTargetSchema();

    /**
     * Judge whether the current schema is equal to the target schema.
     *
     * @return true if the current schema is equal to the target schema, false otherwise.
     */
    boolean hasMapped();

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

//    /**
//     * Construct the mapping of each attributes in this schema with the given ordered list of transforms.
//     *
//     * @param transforms the list of transformations used to construct the mapping.
//     */
//    void constructSchemaMapping(List<Transform> transforms);

    void updateSchemaMappingNodes();

    /**
     * Create a new schema mapping instance using the parameters of this instance.
     * @return
     */
    SchemaMapping createSchemaMapping();

    void updateMapping(Attribute sourceAttribute, Attribute targetAttribute);

    void updateSchema(Schema latestSchema);

    void updateSchema();
}
