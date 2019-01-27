package de.hpi.isg.dataprep.model.target.schema;

/**
 * @author lan.jiang
 * @since 1/22/19
 */
abstract public class Transform {

    /**
     * Using this transform to build a step in the given schema mapping.
     * @param schemaMapping
     */
    abstract public void buildStep(SchemaMapping schemaMapping);
}
