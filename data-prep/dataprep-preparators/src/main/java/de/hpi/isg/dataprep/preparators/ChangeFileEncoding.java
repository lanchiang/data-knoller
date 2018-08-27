package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.ChangeFileEncodingImpl;
import de.hpi.isg.dataprep.model.prepmetadata.ChangeFileEncodingMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class ChangeFileEncoding extends Preparator {

    private String sourceEncoding;
    private String targetEncoding;

    public ChangeFileEncoding(ChangeFileEncodingImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {

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
