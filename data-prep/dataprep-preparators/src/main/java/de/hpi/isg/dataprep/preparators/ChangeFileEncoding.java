package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.implementation.ChangeFileEncodingImpl;
import de.hpi.isg.dataprep.model.metadata.ChangeFileEncodingMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class ChangeFileEncoding extends Preparator {

    private String sourceEncoding;
    private String targetEncoding;

    private ChangeFileEncodingImpl impl;

    public ChangeFileEncoding(String targetEncoding) {
        this.targetEncoding = targetEncoding;
    }

    public ChangeFileEncoding(String sourceEncoding, String targetEncoding) {
        this(targetEncoding);
        this.sourceEncoding = sourceEncoding;
    }

    public ChangeFileEncoding(ChangeFileEncodingImpl impl) {
        this.impl = impl;
        prerequisites = new ChangeFileEncodingMetadata();
    }

    @Override
    protected void executePreparator() throws Exception {
        impl.execute(this);
    }

    @Override
    protected void recordErrorLog() {

    }

    @Override
    protected void recordProvenance() {

    }

    public String getSourceEncoding() {
        return sourceEncoding;
    }

    public void setSourceEncoding(String sourceEncoding) {
        this.sourceEncoding = sourceEncoding;
    }

    public String getTargetEncoding() {
        return targetEncoding;
    }

    public void setTargetEncoding(String targetEncoding) {
        this.targetEncoding = targetEncoding;
    }
}
