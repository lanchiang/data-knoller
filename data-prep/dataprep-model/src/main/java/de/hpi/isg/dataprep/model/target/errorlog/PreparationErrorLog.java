package de.hpi.isg.dataprep.model.target.errorlog;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class PreparationErrorLog extends ErrorLog {

    private AbstractPreparation preparation;

    /**
     * The value that causes this error.
     */
    private String value;

    public PreparationErrorLog(String value, Throwable errorType) {
        super(errorType);
        this.value = value;
    }

    public PreparationErrorLog(AbstractPreparation preparation, String value, Throwable errorType) {
        this(value, errorType);
        this.preparation = preparation;
    }

    public AbstractPreparation getPreparation() {
        return preparation;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreparationErrorLog that = (PreparationErrorLog) o;
        return Objects.equals(preparation, that.preparation) &&
                Objects.equals(value, that.value) && super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(preparation, value, errorType, errorMessage);
    }

    @Override
    public String toString() {
        return "PreparationErrorLog{" +
                "preparation=" + preparation +
                ", value='" + value + '\'' +
                ", errorType='" + errorType + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}