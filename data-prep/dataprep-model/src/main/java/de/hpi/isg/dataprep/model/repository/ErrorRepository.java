package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.ErrorLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/8
 */
public class ErrorRepository {

    private List<ErrorLog> errorLogs;

    public ErrorRepository() {
        this.errorLogs = new ArrayList<>();
    }

    public boolean addErrorLog(ErrorLog errorLog) {
        this.errorLogs.add(errorLog);
        return true;
    }

    public boolean addErrorLogs(Collection<ErrorLog> errorLogs) {
        this.errorLogs.addAll(errorLogs);
        return true;
    }
}
