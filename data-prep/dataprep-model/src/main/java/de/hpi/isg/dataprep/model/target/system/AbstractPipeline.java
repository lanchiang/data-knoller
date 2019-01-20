package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.model.target.data.ColumnCombination;
import de.hpi.isg.dataprep.util.Nameable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collection;
import java.util.List;

/**
 * The abstract class of a data preparation pipeline.
 *
 * @author Lan Jiang
 * @since 2018/9/10
 */
public interface AbstractPipeline extends Nameable {

    /**
     * Add a {@link AbstractPreparation} to this pipeline.
     *
     * @param preparation
     */
    void addPreparation(AbstractPreparation preparation);

    /**
     * Check whether there are pipeline syntax errors during compilation before starting to execute the pipeline.
     * If there is at least one error, return by throwing a {@link PipelineSyntaxErrorException}, otherwise clear the metadata repository.
     *
     * @throws PipelineSyntaxErrorException
     */
    void checkPipelineErrors() throws PipelineSyntaxErrorException;

    /**
     * Entrance of the execution of the data preparation pipeline.
     *
     * @throws Exception
     */
    void executePipeline() throws Exception;

    /**
     * Insert the metadata whose values are already known into the {@link MetadataRepository}
     */
    void initMetadataRepository();

    /**
     * Configure the {@link de.hpi.isg.dataprep.model.target.objects.Metadata} prerequisites used when checking metadata, as well as
     * the set of metadata that will be modified after executing a preparator successfully.
     */
    void buildMetadataSetup();

//    /**
//     * Build the set of {@link ColumnCombination}s for the dataset used in this pipeline.
//     */
//    void buildColumnCombination();

    /**
     * Add the preparation that recommended by the decision engine at the end of the pipeline.
     */
    void addRecommendedPreparation();

    /**
     * Execute the recommended preparator that is added into this pipeline. Followed by this execution, data, metadata
     * and other dynamic information must be updated.
     */
    void executeRecommendedPreparation();

    List<AbstractPreparation> getPreparations();

    ErrorRepository getErrorRepository();

    MetadataRepository getMetadataRepository();

    ProvenanceRepository getProvenanceRepository();

    Dataset<Row> getRawData();

    Collection<ColumnCombination> getColumnCombinations();

    String getDatasetName();

    void setRawData(Dataset<Row> rawData);
}
