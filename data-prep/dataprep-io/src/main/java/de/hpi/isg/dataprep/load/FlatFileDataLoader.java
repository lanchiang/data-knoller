package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.util.dialects.DialectBuilder;
import de.hpi.isg.dataprep.util.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.util.context.DataContext;
import org.apache.spark.sql.DataFrameReader;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
public class FlatFileDataLoader extends SparkDataLoader {

    private String delimiter = ",";
    private String quote = "\"";
    private String escape = "\\";
    private boolean hasHeader = true;

    public FlatFileDataLoader(String sparkAppName, String masterUrl) {
        super(sparkAppName, masterUrl);
    }

    public FlatFileDataLoader(String sparkAppName, String masterUrl, String url) {
        super(sparkAppName, masterUrl, url);
    }

    @Override
    public void load() {
        addOption("sep", delimiter).addOption("quote", quote).addOption("escape", escape).addOption("header", hasHeader?"true":"false");

        DataFrameReader dataFrameReader = super.createDataFrameReader();

        FileLoadDialect dialect =
                new DialectBuilder()
                .delimiter(delimiter)
                .quoteChar(quote)
                .escapeChar(escape)
                .buildDialect();

        dataContext = new DataContext(dataFrameReader.csv(url), dialect);
    }

    /**
     * If delimiter is not specified, try to infer it.
     */
    private void inferDelimieter() {
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getEscape() {
        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }
}
