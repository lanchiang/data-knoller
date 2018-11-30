package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.Printer.ErrorLogPrinter;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.util.PrettyPrintable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * The error log repository that holds all logs of errors in a preparation pipeline.
 * @author Lan Jiang
 * @since 2018/8/8
 */
public class ErrorRepository implements PrettyPrintable {

    private List<ErrorLog> errorLogs;

    public ErrorRepository() {
        this.errorLogs = new ArrayList<>();
    }

    public ErrorRepository(List<ErrorLog> errorLogs) {
        this.errorLogs = errorLogs;
    }

    public void addErrorLog(ErrorLog errorLog) {
        this.errorLogs.add(errorLog);
    }

    public void addErrorLogs(Collection<ErrorLog> errorLogs) {
        this.errorLogs.addAll(errorLogs);
    }

    public List<ErrorLog> getErrorLogs() {
        return errorLogs;
    }

    @Override
    public List<String> getPrintedReady() {
        ErrorLogPrinter errorLogPrinter = new ErrorLogPrinter(this);
        List<String> printReadyErrorLogs = errorLogPrinter.printErrorLogs();
        return printReadyErrorLogs;
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
