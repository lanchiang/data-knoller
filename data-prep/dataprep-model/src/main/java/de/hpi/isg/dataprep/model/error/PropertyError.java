package de.hpi.isg.dataprep.model.error;

/**
 * @author Lan Jiang
 * @since 2018/8/22
 */
public class PropertyError extends PreparationError {

    private String property;

    private Throwable throwable;

    public PropertyError(String property, Throwable throwable) {
        this.property = property;
        this.throwable = throwable;
        this.setErrorLevel(ErrorLevel.PROPERTY);
    }

    public String getProperty() {
        return property;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getErrorProducer() {
        return this.toString();
    }
}
