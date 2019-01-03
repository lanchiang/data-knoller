package de.hpi.isg.dataprep.util;

/**
 * @author Lan Jiang
 * @since 2018/8/24
 */
public enum MetadataEnum {
    FILE_ENCODING("file-encoding"),
    DELIMITER("delimiter"),
    ESCAPE_CHARACTERS("escape-characters"),
    QUOTE_CHARACTER("quote-character"),
    PROPERTY_DATA_TYPE("property-datatype"),
    PROPERTY_DATE_PATTERN("property-datepattern");

    private String metadata;

    MetadataEnum(String string) {
        this.metadata = string;
    }

    public String getMetadata() {
        return metadata;
    }
}