package de.hpi.isg.dataprep.model.target.errorlog;

import de.hpi.isg.dataprep.model.target.system.Pipeline;
import de.hpi.isg.dataprep.model.target.system.Preparation;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class PipelineErrorLog extends ErrorLog {

    private Pipeline pipeline;

    private Preparation current;
    private Preparation previous;

    public PipelineErrorLog(Pipeline pipeline, Throwable throwable) {
        super(throwable);
        this.pipeline = pipeline;
    }

    public PipelineErrorLog(Pipeline pipeline, Preparation preparation, Throwable throwable) {
        this(pipeline, throwable);

        current = preparation;
        int previousIndex = current.getPosition() - 1;
        previous = (previousIndex == -1) ? null : pipeline.getPreparations().stream().filter(prep -> prep.getPosition() == previousIndex).findFirst().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PipelineErrorLog that = (PipelineErrorLog) o;
        return Objects.equals(pipeline, that.pipeline) &&
                Objects.equals(current, that.current) &&
                Objects.equals(previous, that.previous) && super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipeline, current, previous);
    }

    @Override
    public String toString() {
        return "PipelineErrorLog{" +
                "pipeline=" + pipeline +
                ", current=" + current +
                ", previous=" + previous +
                ", errorType='" + errorType + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
