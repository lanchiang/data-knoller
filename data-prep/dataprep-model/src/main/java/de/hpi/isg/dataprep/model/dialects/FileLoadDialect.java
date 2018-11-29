package de.hpi.isg.dataprep.model.dialects;

import java.io.Serializable;

/**
 * @author Lan Jiang
 * @since 2018/9/9
 */
public class FileLoadDialect implements Serializable {

    private String tableName;

    private String delimiter;
    private String quoteChar;
    private String escapeChar;
    private String lineTerminator;
    private String hasHeader;
    private QuoteBehavior quoteBehavior = QuoteBehavior.QUOTE_MINIMAL;

    private String sparkAppName;
    private String masterUrl;
    private String url;

    private String inferSchema;

    public enum QuoteBehavior {
        QUOTE_ALL,
        QUOTE_MINIMAL,
        QUOTE_NONNUMERIC,
        QUOTE_NONE;
    }

    public FileLoadDialect() {
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public String getEscapeChar() {
        return escapeChar;
    }

    public void setEscapeChar(String escapeChar) {
        this.escapeChar = escapeChar;
    }

    public String getLineTerminator() {
        return lineTerminator;
    }

    public void setLineTerminator(String lineTerminator) {
        this.lineTerminator = lineTerminator;
    }

    public QuoteBehavior getQuoteBehavior() {
        return quoteBehavior;
    }

    public void setQuoteBehavior(QuoteBehavior quoteBehavior) {
        this.quoteBehavior = quoteBehavior;
    }

    public String getSparkAppName() {
        return sparkAppName;
    }

    public void setSparkAppName(String sparkAppName) {
        this.sparkAppName = sparkAppName;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(String hasHeader) {
        this.hasHeader = hasHeader;
    }

    public String getInferSchema() {
        return inferSchema;
    }

    public void setInferSchema(String inferSchema) {
        this.inferSchema = inferSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
