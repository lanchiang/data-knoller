package de.hpi.isg.dataprep.Printer;

import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The utility class to print well-organized error logs.
 *
 * @author Lan Jiang
 * @since 2018/9/12
 */
public class ErrorLogPrinter {

    private ErrorRepository errorRepository;

    public ErrorLogPrinter(ErrorRepository errorRepository) {
        this.errorRepository = errorRepository;
    }

    public List<String> printErrorLogs() {
        List<String> printed = errorRepository.getErrorLogs().stream()
                .map(errorLog -> printErrorLog(errorLog))
                .collect(Collectors.toList());
        return printed;
    }

    private <T extends ErrorLog> String printErrorLog(T errorLog) {
        StringBuffer stringBuffer = new StringBuffer();
        if (errorLog.getClass().isAssignableFrom(PipelineErrorLog.class)) {
            PipelineErrorLog casted = (PipelineErrorLog) errorLog;
            AbstractPipeline pipeline = casted.getPipeline();
            int position = casted.getCurrent().getPosition();
            String errorType = casted.getErrorType();
            String errorMessage = casted.getErrorMessage();

            stringBuffer.append("{Pipeline: ").append(pipeline.getName()).append(", ");
            stringBuffer.append("Position: ").append(position).append(", ");
            stringBuffer.append("Error type: ").append(errorType).append(", Error message: ").append(errorMessage).append("}");
        } else if (errorLog.getClass().isAssignableFrom(PreparationErrorLog.class)) {
            PreparationErrorLog casted = (PreparationErrorLog) errorLog;
            AbstractPreparation preparation = casted.getPreparation();
            String value = casted.getValue();
            String errorType = casted.getErrorType();
            String errorMessage = casted.getErrorMessage();

            stringBuffer.append("{Preparation: ").append(preparation.getName()).append(", ");
            stringBuffer.append("Value: ").append(value).append(", ");
            stringBuffer.append("Error type: ").append(errorType).append(", Error message: ").append(errorMessage).append("}");
        }
        return stringBuffer.toString();
    }
}
