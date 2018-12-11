package de.hpi.isg.dataprep.model.objects.errorlog;

import de.hpi.isg.dataprep.model.objects.Target;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class ErrorLog extends Target {

    /**
     * The exception describes the error. One might extract error message from this exception.
     */
    protected Throwable error;

    /**
     * The error type is currently equal to the {@link Throwable} class name.
     */
    protected String errorType;

    /**
     * The detail of this error.
     */
    protected String errorMessage;

    public ErrorLog(Throwable error) {
        this.error = error;
        this.errorType = error.getClass().getSimpleName();
        this.errorMessage = error.getMessage();
    }

    public Throwable getError() {
        return error;
    }

    public String getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorLog errorLog = (ErrorLog) o;
        return Objects.equals(errorType, errorLog.errorType) &&
                Objects.equals(errorMessage, errorLog.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorType, errorMessage);
    }
}