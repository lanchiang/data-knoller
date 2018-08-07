package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.metadata.ChangeFileEncodingMetadata;
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
    public boolean checkMetadata() {
        prerequisites = new ChangeFileEncodingMetadata();
        for (String metadata : prerequisites.getPrerequisites()) {

        }
        return false;
    }

    @Override
    public void executePreparator() {

    }

    @Override
    public void recordErrorLog() {

    }

    @Override
    public void recordProvenance() {

    }
}
