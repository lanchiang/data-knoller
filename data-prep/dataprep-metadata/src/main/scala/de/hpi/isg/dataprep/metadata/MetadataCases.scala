package de.hpi.isg.dataprep.metadata

import de.hpi.isg.dataprep.metadata.MetadataScope.MetadataScope

/**
  * Case classes of all the supported metadata. Todo: the class name must be changed to Metadata after the old metadata class is removed.
  *
  * @author Lan Jiang
  * @since 2019-05-14
  */
abstract class MetadataCases(metadataScope: MetadataScope = MetadataScope.PropertyMetadata) {}

object MetadataScope extends Enumeration {
  type MetadataScope = Value
  val PropertyMetadata, TableMetadata, FileMetadata = Value
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Property
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Property(name: String) extends MetadataCases
case class PropertyNullable(property: Property, nullable: Boolean) extends MetadataCases
case class PropertyDataType(property: Property, dataType: String) extends MetadataCases
case class EscapeCharacters(property: Property, escapeCharacter: String) extends MetadataCases
case class Language(property: Property, language: String) extends MetadataCases
case class Position(property: Property, position: Int) extends MetadataCases // indicates the position of this property in the schema.

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Table(name: String, properties: Array[Property]) extends MetadataCases(MetadataScope.TableMetadata)
case class HeaderExistence(table: Table, hasHeader: Boolean) extends MetadataCases(MetadataScope.TableMetadata)

////////////////////////////////////////////////////////////////////////////////////////////////////////
// File
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class DataFile(name: String, table: Table) extends MetadataCases(MetadataScope.FileMetadata) // Currently, one file contains data of only one table.
case class Delimiter(file: DataFile, delimiter: String) extends MetadataCases(MetadataScope.FileMetadata)
case class QuoteCharacters(file: DataFile, quote: String) extends MetadataCases(MetadataScope.FileMetadata)
case class LineEndCharacters(file: DataFile, endLineCharacters: String) extends MetadataCases(MetadataScope.FileMetadata)
case class Encoding(file: DataFile, encoding: String) extends MetadataCases(MetadataScope.FileMetadata)

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Statistics
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Max(property: Property, max: Object) extends MetadataCases
case class Min(property: Property, min: Object) extends MetadataCases
case class Distinct(property: Property, numDistinct: Long) extends MetadataCases

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Pipeline
////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Value format
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class DateFormat(property: Property, dateFormat: String) extends MetadataCases
case class PhoneFormat(property: Property, phoneFormat: String) extends MetadataCases