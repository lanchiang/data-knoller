package de.hpi.isg.dataprep.util;

/**
 * @author Lan Jiang
 * @since 2018/8/24
 */
public enum Metadata {
    FILE_ENCODING("file-encoding"),
    DELIMITER("delimiter"),
    ESCAPE_CHARACTERS("escape-characters"),
    QUOTE_CHARACTER("quote-character"),
    PROPERTY_DATATYPE("property-datatype");

    private String metadata;

    Metadata(String string) {
        this.metadata = string;
    }

    public String getMetadata() { return metadata;}
}
