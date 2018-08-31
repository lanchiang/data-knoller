package de.hpi.isg.dataprep.model.error;

import java.io.Serializable;

/**
 * @author Lan Jiang
 * @since 2018/8/22
 */
abstract public class PreparationError implements Serializable {

    private static final long serialVersionUID = 672917503135516045L;

    public enum ErrorLevel {
        RECORD,
        PROPERTY,
        DATASET
    }

    private ErrorLevel errorLevel;

    public ErrorLevel getErrorLevel() {
        return errorLevel;
    }

    public void setErrorLevel(ErrorLevel errorLevel) {
        this.errorLevel = errorLevel;
    }
}
