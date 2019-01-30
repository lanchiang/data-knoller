package de.hpi.isg.dataprep.model.target.schema;

import java.util.Set;

/**
 * This interface provides the methods that a concrete schema mapping class must implement. It is used by the decision engine
 * to calculate the applicability score for preparator suggestion.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
abstract public class SchemaMapping {

    abstract public Schema getCurrentSchema();

    abstract public void setCurrentSchema(Schema schema);

    abstract public Schema getTargetSchema();

    /**
     * Check whether the given attribute has been mapped in the target schema.
     *
     * @return true if the current schema is equal to the target schema, false otherwise.
     */
    abstract protected boolean hasMapped(Attribute attribute);

    /**
     * Check whether the target schema is the same as the current schema. Maybe not keep.
     * @return
     */
    abstract public boolean hasMapped();

    /**
     * Get the set of attributes in the target schema that are derived from the given {@link Attribute}. This method can be used
     * in calApplicability to get the attributes in targetSchema that the given column combination still need to be mapped.
     *
     * @param attribute the attribute in the current schema
     * @return the set of attributes in the target schema that are derived from the given attribute. If the given attribute
     * does not exist in the source schema, return null.
     */
    abstract public Set<Attribute> getTargetBySourceAttribute(Attribute attribute);

    /**
     * Get the set of attributes in the target schema that are derived from the given {@link Attribute}, which is specified
     * by its attribute name. This method can be used in calApplicability to get the attributes in targetSchema that the given
     * column combination still need to be mapped.
     *
     * @param attributeName the name of the attribute in the current schema.
     * @return the set of attributes in the target schema that are derived from the given attribute. If the given attribute
     * does not exist in the source schema, return null.
     */
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

    abstract protected void finalizeUpdate();

    /**
     * Create a new schema mapping instance using the parameters of this instance.
     * @return
     */
    abstract protected SchemaMapping createSchemaMapping();

    abstract protected void updateMapping(Attribute sourceAttribute, Attribute targetAttribute);

    abstract protected void updateSchema();

    abstract protected void print();
}
