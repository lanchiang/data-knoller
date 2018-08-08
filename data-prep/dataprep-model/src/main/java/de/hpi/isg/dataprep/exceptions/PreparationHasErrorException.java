package de.hpi.isg.dataprep.exceptions;

/**
 * @author Lan Jiang
 * @since 2018/8/8
 */
public class PreparationHasErrorException extends RuntimeException {

    public PreparationHasErrorException(String message) {
        super(message);
    }
}
