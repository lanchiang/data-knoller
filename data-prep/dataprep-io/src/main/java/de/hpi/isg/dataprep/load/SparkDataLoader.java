package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.util.context.DataContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
abstract public class SparkDataLoader {

    // the instance to keep loaded dataset from supported source.
    protected DataContext dataContext;

    protected String sparkAppName = "Default data preparation task";
    protected String masterUrl = "local";
    protected String url;
    protected Map<String, String> options;

    protected String encoding;


    public SparkDataLoader() {
        options = new HashMap<>();
    }

    public SparkDataLoader(String sparkAppName, String masterUrl) {
        this();
        this.sparkAppName = sparkAppName;
        this.masterUrl = masterUrl;
    }

    public SparkDataLoader(String sparkAppName, String masterUrl, String url) {
        this(sparkAppName, masterUrl);
        this.url = url;
    }

    public SparkDataLoader addOption(String key, String value) {
        options.put(key, value);
        return this;
    }

    protected DataFrameReader createDataFrameReader() {
        DataFrameReader dataFrameReader = SparkSession.builder().appName(sparkAppName).master(masterUrl).getOrCreate().read();

        for (Map.Entry<String, String> option : options.entrySet()) {
            dataFrameReader = dataFrameReader.option(option.getKey(), option.getValue());
        }
        return dataFrameReader;
    }

    abstract public void load();

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public DataContext getDataContext() {
        return dataContext;
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
