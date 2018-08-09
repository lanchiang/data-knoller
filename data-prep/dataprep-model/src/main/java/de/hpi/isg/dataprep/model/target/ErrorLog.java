package de.hpi.isg.dataprep.model.target;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class ErrorLog extends Target {

    /**
     * This enumeration variable encapsulates the types of errors that might occur.
     */
    enum ErrorType {
        ValueNotConvertible
    }

    /**
     * Indicates which {@link PipelineComponent} causes this error.
     * The instance of {@link Pipeline} means it is a pipeline level error.
     * The instance of {@link Preparation} means it is an error caused by a specific preparation.
     */
    private PipelineComponent pipelineComponent;

    /**
     * Indicates the type of this error.
     */
    private ErrorType errorType;

    /**
     * The unconvertible value.
     */
    private String value;

    public ErrorLog(String value, Throwable exception) {
        this.value = value;
        if (exception instanceof NumberFormatException) {
            this.errorType = ErrorType.ValueNotConvertible;
        }
    }

    public PipelineComponent getPipelineComponent() {
        return pipelineComponent;
    }

    public void setPipelineComponent(Preparation pipelineComponent) {
        this.pipelineComponent = pipelineComponent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorLog errorLog = (ErrorLog) o;
        return Objects.equals(pipelineComponent, errorLog.pipelineComponent) &&
                errorType == errorLog.errorType &&
                Objects.equals(value, errorLog.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipelineComponent, errorType, value);
    }
}
