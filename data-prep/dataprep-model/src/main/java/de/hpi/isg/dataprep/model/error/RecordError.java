package de.hpi.isg.dataprep.model.error;

import java.io.Serializable;

/**
 * @author Lan Jiang
 * @since 2018/8/22
 */
public class RecordError extends PreparationError {

    private String errRecord;

    private Throwable error;

    public RecordError(String errRecord, Throwable error) {
        this.errRecord = errRecord;
        this.error = error;
        this.setErrorLevel(ErrorLevel.RECORD);
    }

    public String getErrRecord() {
        return errRecord;
    }

    public Throwable getError() {
        return error;
    }

}
