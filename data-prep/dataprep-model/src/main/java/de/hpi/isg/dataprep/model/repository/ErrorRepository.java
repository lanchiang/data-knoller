package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * The error log repository that holds all logs of errors in a preparation pipeline.
 * @author Lan Jiang
 * @since 2018/8/8
 */
public class ErrorRepository {

    private List<ErrorLog> errorLogs;

    public ErrorRepository() {
        this.errorLogs = new ArrayList<>();
    }

    public ErrorRepository(List<ErrorLog> errorLogs) {
        this.errorLogs = errorLogs;
    }

    public boolean addErrorLog(ErrorLog errorLog) {
        this.errorLogs.add(errorLog);
        return true;
    }

    public boolean addErrorLogs(Collection<ErrorLog> errorLogs) {
        this.errorLogs.addAll(errorLogs);
        return true;
    }

    public List<ErrorLog> getErrorLogs() {
        return errorLogs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorRepository that = (ErrorRepository) o;
        return Objects.equals(errorLogs, that.errorLogs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorLogs);
    }
}
