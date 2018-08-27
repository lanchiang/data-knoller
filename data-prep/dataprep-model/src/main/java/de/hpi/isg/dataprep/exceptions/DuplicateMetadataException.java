package de.hpi.isg.dataprep.exceptions;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class DuplicateMetadataException extends RuntimeMetadataException {

    public DuplicateMetadataException(String message) {
        super(message);
    }
}
