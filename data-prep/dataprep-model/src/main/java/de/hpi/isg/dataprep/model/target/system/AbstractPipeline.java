package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.util.Nameable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/9/10
 */
public interface AbstractPipeline extends Nameable {
    
    void addPreparation(AbstractPreparation preparation);

    /**
     * Check whether there are pipeline syntax errors during compilation before starting to execute the pipeline.
     * If there is at least one error, return by throwing a {@link PipelineSyntaxErrorException}, otherwise clear the metadata repository.
     *
     * @throws PipelineSyntaxErrorException
     */
    void checkPipelineErrors() throws PipelineSyntaxErrorException;

    void executePipeline() throws Exception;

    void initMetadataRepository();

    List<AbstractPreparation> getPreparations();

    ErrorRepository getErrorRepository();

    MetadataRepository getMetadataRepository();

    ProvenanceRepository getProvenanceRepository();

    Dataset<Row> getRawData();

    void setRawData(Dataset<Row> rawData);
}
