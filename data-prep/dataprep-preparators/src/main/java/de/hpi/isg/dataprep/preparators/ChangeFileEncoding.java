package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.model.metadata.ChangeFileEncodingMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class ChangeFileEncoding extends Preparator {

    private String targetEncoding;

    public ChangeFileEncoding(String targetEncoding) {
        this.targetEncoding = targetEncoding;
    }

    public ChangeFileEncoding(String sourceEncoding, String targetEncoding) {
        this(targetEncoding);
    }

    @Override
    protected boolean checkMetadata() {
        prerequisites = new ChangeFileEncodingMetadata();
        for (String metadata : prerequisites.getPrerequisites()) {
            if (false) {
                // this metadata is not satisfied, add it to the invalid metadata set.
                this.getInvalid().add(new Metadata(metadata));
                return false;
            }
        }
        return true;
    }

    @Override
    protected void executePreparator() throws Exception {
        Dataset<Row> dataset = this.getPreparation().getPipeline().getRawData();

        this.setUpdatedDataset(dataset);
    }

    @Override
    protected void recordErrorLog() {

    }

    @Override
    protected void recordProvenance() {

    }
}
