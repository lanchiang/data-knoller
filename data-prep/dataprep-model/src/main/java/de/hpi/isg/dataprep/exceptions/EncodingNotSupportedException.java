package de.hpi.isg.dataprep.exceptions;

public class EncodingNotSupportedException extends RuntimeException {

    private static final long serialVersionUID = -660506907039679878L;

    public EncodingNotSupportedException(String message) {
        super(message);
    }
}
