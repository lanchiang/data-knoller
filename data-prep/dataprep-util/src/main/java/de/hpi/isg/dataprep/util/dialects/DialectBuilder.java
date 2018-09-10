package de.hpi.isg.dataprep.util.dialects;

/**
 * The builder of a {@link FileLoadDialect}
 *
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class DialectBuilder {

    private String delimiter = ",";
    private String quoteChar = "\"";
    private String escapeChar = "\\";
    private String lineTerminator = "\r\n";
    private FileLoadDialect.QuoteBehavior quoteBehavior = FileLoadDialect.QuoteBehavior.QUOTE_MINIMAL;

    public DialectBuilder() {}

    public FileLoadDialect buildDialect() {
        return new FileLoadDialect(delimiter, quoteChar, escapeChar, lineTerminator, quoteBehavior);
    }

    public DialectBuilder delimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public DialectBuilder quoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    public DialectBuilder escapeChar(String escapeChar) {
        this.escapeChar = escapeChar;
        return this;
    }

    public DialectBuilder lineTerminator(String lineTerminator) {
        this.lineTerminator = lineTerminator;
        return this;
    }

    public DialectBuilder quoteBehavior(FileLoadDialect.QuoteBehavior quoteBehavior) {
        this.quoteBehavior = quoteBehavior;
        return this;
    }
}
