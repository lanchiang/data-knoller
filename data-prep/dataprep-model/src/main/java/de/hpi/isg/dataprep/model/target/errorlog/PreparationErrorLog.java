package de.hpi.isg.dataprep.model.target.errorlog;

import de.hpi.isg.dataprep.model.target.Preparation;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class PreparationErrorLog extends ErrorLog {

    private Preparation preparation;

    /**
     * The value that causes this error.
     */
    private String value;

    public PreparationErrorLog(String value, Throwable errorType) {
        super(errorType);
        this.value = value;
    }

    public PreparationErrorLog(Preparation preparation, String value, Throwable errorType) {
        this(value, errorType);
        this.preparation = preparation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparationErrorLog that = (PreparationErrorLog) o;
        return Objects.equals(preparation, that.preparation) &&
                Objects.equals(value, that.value) &&
                Objects.equals(errorType, that.errorType) &&
                Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(preparation, value, errorType, errorMessage);
    }
}