package de.hpi.isg.dataprep.util;

import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/6/5
 */
public class DatasetConfig {

    private String appName;
    private String filePath;
    private Map<String, String> options;
    private Master master;
    private InputFileFormat inputFileFormat;

    /**
     * The master node of the spark task.
     */
    public enum Master {
        local
    }

    public enum InputFileFormat {
        CSV,
        TEXTFILE
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public String getMaster() {
        return master.toString();
    }

    public void setMaster(Master master) {
        this.master = master;
    }

    public InputFileFormat getInputFileFormat() {
        return inputFileFormat;
    }

    public void setInputFileFormat(InputFileFormat inputFileFormat) {
        this.inputFileFormat = inputFileFormat;
    }
}
