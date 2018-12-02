package de.hpi.isg.dataprep.exceptions;

public class EncodingNotDetectedException extends RuntimeMetadataException {

    public EncodingNotDetectedException(String fileName) {
        super(String.format("Could not detect the encoding of the file '%s'", fileName));
    }
}
