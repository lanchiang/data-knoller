package de.hpi.isg.dataprep.model.target.errorlog;

import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;

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
        this.error = throwable;
    }

    public PipelineErrorLog(Pipeline pipeline, Preparation current, Throwable throwable) {
        this(pipeline, throwable);
        this.current = current;
    }

    public PipelineErrorLog(Pipeline pipeline, Preparation current, Preparation previous, Throwable error) {
        this(pipeline, current, error);
        this.pipeline = pipeline;
        this.current = current;
        this.previous = previous;
    }
}
