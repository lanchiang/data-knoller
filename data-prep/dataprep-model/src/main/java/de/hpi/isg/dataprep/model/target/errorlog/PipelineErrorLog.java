package de.hpi.isg.dataprep.model.target.errorlog;

import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class PipelineErrorLog extends ErrorLog {

    public Pipeline pipeline;

    public Preparation preparation;

    public PipelineErrorLog(Pipeline pipeline, Throwable throwable) {
        super(throwable);
        this.pipeline = pipeline;
        this.error = throwable;
    }

    public PipelineErrorLog(Pipeline pipeline, Preparation preparation, Throwable throwable) {
        this(pipeline, throwable);
        this.preparation = preparation;
    }
}
