package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.util.Executable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * This interface defines all the common behaviors of every preparator.
 *
 * @author Lan Jiang
 * @since 2018/9/17
 */
public interface AbstractPreparator extends Executable {

    /**
     * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
     * prerequisite and toChange set.
     *
     * @throws ParameterNotSpecifiedException
     */
    void buildMetadataSetup() throws ParameterNotSpecifiedException;

    /**
     * This method checks whether values of the prerequisite metadata are met. It shall preserve the unsatisfying metadata.
     */
    void checkMetadataPrerequisite();

    /**
     * After the execution of this preparator finishes, call this method to post config the pipeline.
     * For example, update the dataset, update the metadata repository.
     */
    void postExecConfig();

//    /**
//     * This method calculates the applicability score of this preparator on all column(s). The score represents how suitable this preparator is on
//     * the specified column(s), ranging from 0 to 1. A higher score means the preparator is more suitable for the column(s), and vice versa.
//     * @return the score array of this preparator on all the column(s).
//     */
//    Double[] calApplicability();

    /**
     * Execute this preparator.
     * @throws Exception
     */
    @Override
    void execute() throws Exception;

    AbstractPreparation getPreparation();

    List<Metadata> getInvalidMetadata();

    List<Metadata> getPrerequisiteMetadata();

    List<Metadata> getUpdateMetadata();

    void setUpdatedTable(Dataset<Row> updatedTable);

    void setPreparation(AbstractPreparation preparation);
}
