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
abstract public class SchemaMapping {

    abstract public Schema getSourceSchema();

    abstract public Schema getCurrentSchema();

    abstract public Schema getTargetSchema();

    /**
     * Judge whether the current schema is equal to the target schema.
     *
     * @return true if the current schema is equal to the target schema, false otherwise.
     */
    abstract public boolean hasMapped();

    /**
     * Check whether the given attribute is in the source schema.
     *
     * @param attribute the attribute to be checked.
     * @return
     */
    abstract public boolean isInSource(Attribute attribute);

    /**
     * Check whether each of the given attributes is in the source schema.
     * @param attributes the array of attributes to be checked.
     * @return
     */
    abstract public boolean isInSource(Attribute[] attributes);

    /**
     * Get the set of attributes in the target schema that are derived from the given {@link Attribute}.
     *
     * @param attribute in the source schema
     * @return the set of attributes in the target schema that are derived from the given attribute. If
     * the given attribute does not exist in the source schema, return null.
     */
    abstract public Set<Attribute> getTargetBySourceAttribute(Attribute attribute);

    abstract public Set<Attribute> getTargetBySourceAttributeName(String attributeName);

//    /**
//     * Get the set of attributes in the source schema that derive the given target {@link Attribute}.
//     *
//     * @param attribute in the target schema
//     * @return the set of attributes in the source schema that derive the given target attribute. If the
//     * given attribute is not derived from any attribute in the source schema, return null.
//     */
//    Set<Attribute> getSourceByTargetAttribute(Attribute attribute);

//    Set<Attribute> getSourceByTargetAttributeName(String attributeName);

//    /**
//     * Construct the mapping of each attributes in this schema with the given ordered list of transforms.
//     *
//     * @param transforms the list of transformations used to construct the mapping.
//     */
//    void constructSchemaMapping(List<Transform> transforms);


    abstract protected void finalizeUpdate();

    /**
     * Create a new schema mapping instance using the parameters of this instance.
     * @return
     */
    abstract protected SchemaMapping createSchemaMapping();

    abstract protected void updateMapping(Attribute sourceAttribute, Attribute targetAttribute);

    abstract protected void updateSchema(Schema latestSchema);

    abstract protected void updateSchema();

    abstract public void print();
}
