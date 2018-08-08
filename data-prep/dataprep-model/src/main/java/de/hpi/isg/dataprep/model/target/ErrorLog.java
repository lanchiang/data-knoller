package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;

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
     * Indicates which {@link Preparation} causes this error.
     */
    private Preparation preparation;

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

    public Preparation getPreparation() {
        return preparation;
    }

    public void setPreparation(Preparation preparation) {
        this.preparation = preparation;
    }
}
