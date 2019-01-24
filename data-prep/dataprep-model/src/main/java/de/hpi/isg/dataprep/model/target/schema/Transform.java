package de.hpi.isg.dataprep.model.target.schema;

/**
 * @author lan.jiang
 * @since 1/22/19
 */
abstract public class Transform {

//    protected SchemaGeneratorSandbox owner;
//
//    public SchemaGeneratorSandbox getOwner() {
//        return owner;
//    }
//
//    public void setOwner(SchemaGeneratorSandbox owner) {
//        this.owner = owner;
//    }

    abstract public void reformSchema(SchemaMapping schemaMapping);
}
