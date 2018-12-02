package de.hpi.isg.dataprep.exceptions;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class EncodingNotDetectedException extends RuntimeMetadataException {

    public EncodingNotDetectedException(String fileName) {
        super(String.format("Could not detect the encoding of the file '%s'", fileName));
    }
}
