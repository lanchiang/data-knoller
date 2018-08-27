package de.hpi.isg.dataprep.exceptions;

/**
 * @author Lan Jiang
 * @since 2018/8/25
 */
public class PipelineSyntaxErrorException extends RuntimeException {

    public PipelineSyntaxErrorException(String message) {
        super(message);
    }
}
