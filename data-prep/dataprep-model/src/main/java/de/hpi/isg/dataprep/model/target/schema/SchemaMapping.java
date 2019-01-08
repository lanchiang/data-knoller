package de.hpi.isg.dataprep.model.target.schema;

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
}
