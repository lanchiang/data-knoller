package de.hpi.isg.dataprep.util.dialects;

/**
 * @author Lan Jiang
 * @since 2018/9/9
 */
public class FileLoadDialect {

    private String delimiter = ",";
    private String quoteChar = "\"";
    private String escapeChar;
    private String lineTerminator = "\r\n";
    private QuoteBehavior quoteBehavior = QuoteBehavior.QUOTE_MINIMAL;

    public enum QuoteBehavior {
        QUOTE_ALL,
        QUOTE_MINIMAL,
        QUOTE_NONNUMERIC,
        QUOTE_NONE;
    }

    protected FileLoadDialect(String delimiter, String quoteChar, String escapeChar, String lineterminator, QuoteBehavior quoteBehavior) {
        this.delimiter = delimiter;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.lineTerminator = lineterminator;
        this.quoteBehavior = quoteBehavior;
    }
}
