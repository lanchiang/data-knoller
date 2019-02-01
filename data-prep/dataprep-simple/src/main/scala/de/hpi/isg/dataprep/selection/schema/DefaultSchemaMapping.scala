package de.hpi.isg.dataprep.selection.schema

import java.util

import de.hpi.isg.dataprep.model.target.schema.{Attribute, Schema, SchemaMapping}

import scala.collection.JavaConverters._

class DefaultSchemaMapping(sourceSchema: Schema, targetSchema: Schema) extends SchemaMapping {

  override def getCurrentSchema: Schema = ???

  override def setCurrentSchema(schema: Schema): Unit = ???

  override def getTargetSchema: Schema = ???

  /**
    * Check whether the given attribute has been mapped in the target schema.
    *
    * @return true if the current schema is equal to the target schema, false otherwise.
    */
  override protected def hasMapped(attribute: Attribute): Boolean = targetSchema.getAttributes.asScala.exists(_.equals(attribute))

  /**
    * Check whether the target schema is the same as the current schema. Maybe not keep.
    *
    * @return
    */
  override def hasMapped: Boolean = sourceSchema.getAttributes.asScala.forall(hasMapped)

  /**
    * Get the set of attributes in the target schema that are derived from the given {@link Attribute}. This method can be used
    * in calApplicability to get the attributes in targetSchema that the given column combination still need to be mapped.
    *
    * @param attribute the attribute in the current schema
    * @return the set of attributes in the target schema that are derived from the given attribute. If the given attribute
    *         does not exist in the source schema, return null.
    */
  override def getTargetBySourceAttribute(attribute: Attribute): util.Set[Attribute] = ???

  /**
    * Get the set of attributes in the target schema that are derived from the given {@link Attribute}, which is specified
    * by its attribute name. This method can be used in calApplicability to get the attributes in targetSchema that the given
    * column combination still need to be mapped.
    *
    * @param attributeName the name of the attribute in the current schema.
    * @return the set of attributes in the target schema that are derived from the given attribute. If the given attribute
    *         does not exist in the source schema, return null.
    */
  override def getTargetBySourceAttributeName(attributeName: String): util.Set[Attribute] = ???

  override protected def finalizeUpdate(): Unit = ???

  /**
    * Create a new schema mapping instance using the parameters of this instance.
    *
    * @return
    */
  override protected def createSchemaMapping(): SchemaMapping = ???

  override protected def updateMapping(sourceAttribute: Attribute, targetAttribute: Attribute): Unit = ???

  override protected def updateSchema(): Unit = ???

  override protected def print(): Unit = ???
}
