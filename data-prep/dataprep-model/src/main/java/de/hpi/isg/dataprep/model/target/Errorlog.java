package de.hpi.isg.dataprep.model.target;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Errorlog extends Target {

    private String errorType;

    private String originalValue;

    public Errorlog(String errorType, String originalValue) {
        this.errorType = errorType;
        this.originalValue = originalValue;
    }

    @Override
    public String toString() {
        return "Errorlog{" +
                "errorType='" + errorType + '\'' +
                ", originalValue='" + originalValue + '\'' +
                '}';
    }
}
