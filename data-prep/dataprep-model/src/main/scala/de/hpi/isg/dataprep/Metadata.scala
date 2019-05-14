package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.MetadataScope.MetadataScope

/**
  * Case classes of all the supported metadata. Todo: the class name must be changed to MetadataOld after the old metadata class is removed.
  *
  * @author Lan Jiang
  * @since 2019-05-14
  */
sealed abstract class Metadata(metadataScope: MetadataScope = MetadataScope.PropertyMetadata) extends Serializable

object MetadataScope extends Enumeration {
  type MetadataScope = Value
  val PropertyMetadata, TableMetadata, FileMetadata = Value
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Property
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Property(name: String) extends Metadata
case class PropertyNullable(property: Property, nullable: Boolean) extends Metadata
case class PropertyDataType(property: Property, dataType: String) extends Metadata
case class EscapeCharacters(property: Property, escapeCharacter: String) extends Metadata
case class Language(property: Property, language: String) extends Metadata
case class Position(property: Property, position: Int) extends Metadata // indicates the position of this property in the schema.

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Table(name: String, properties: Array[Property]) extends Metadata(MetadataScope.TableMetadata)
case class HeaderExistence(table: Table, hasHeader: Boolean) extends Metadata(MetadataScope.TableMetadata)

////////////////////////////////////////////////////////////////////////////////////////////////////////
// File
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class DataFile(name: String, table: Table) extends Metadata(MetadataScope.FileMetadata) // Currently, one file contains data of only one table.
case class Delimiter(file: DataFile, delimiter: String) extends Metadata(MetadataScope.FileMetadata)
case class QuoteCharacters(file: DataFile, quote: String) extends Metadata(MetadataScope.FileMetadata)
case class LineEndCharacters(file: DataFile, endLineCharacters: String) extends Metadata(MetadataScope.FileMetadata)
case class Encoding(file: DataFile, encoding: String) extends Metadata(MetadataScope.FileMetadata)

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Statistics
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class Max(property: Property, max: Object) extends Metadata
case class Min(property: Property, min: Object) extends Metadata
case class Distinct(property: Property, numDistinct: Long) extends Metadata

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Pipeline
////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////
// Value format
////////////////////////////////////////////////////////////////////////////////////////////////////////

case class DateFormat(property: Property, dateFormat: String) extends Metadata
case class PhoneFormat(property: Property, phoneFormat: String) extends Metadata