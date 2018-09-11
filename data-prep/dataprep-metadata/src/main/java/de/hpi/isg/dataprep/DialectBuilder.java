package de.hpi.isg.dataprep;

import de.hpi.isg.dataprep.metadata.Delimiter;
import de.hpi.isg.dataprep.metadata.EscapeCharacter;
import de.hpi.isg.dataprep.metadata.QuoteCharacter;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;

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
    private String hasHeader = "false";
    private FileLoadDialect.QuoteBehavior quoteBehavior = FileLoadDialect.QuoteBehavior.QUOTE_MINIMAL;

    private String sparkAppName = "Default data preparation";
    private String masterUrl = "local";
    private String url;

    private String inferSchema = "false";

    public FileLoadDialect buildDialect() {
        FileLoadDialect dialect = new FileLoadDialect();

        dialect.setDelimiter(delimiter);
        dialect.setQuoteChar(quoteChar);
        dialect.setEscapeChar(escapeChar);
        dialect.setLineTerminator(lineTerminator);
        dialect.setHasHeader(hasHeader);
        dialect.setQuoteBehavior(quoteBehavior);

        dialect.setSparkAppName(sparkAppName);
        dialect.setMasterUrl(masterUrl);
        dialect.setUrl(url);

        dialect.setInferSchema(inferSchema);

        return dialect;
    }

    public DialectBuilder sparkAppName(String sparkAppName) {
        this.sparkAppName = sparkAppName;
        return this;
    }

    public DialectBuilder masterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
        return this;
    }

    public DialectBuilder url(String url) {
        this.url = url;
        return this;
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

    public DialectBuilder hasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader?"true":"false";
        return this;
    }

    public DialectBuilder quoteBehavior(FileLoadDialect.QuoteBehavior quoteBehavior) {
        this.quoteBehavior = quoteBehavior;
        return this;
    }

    public DialectBuilder inferSchema(boolean inferSchema) {
        this.inferSchema = inferSchema?"true":"false";
        return this;
    }
}
