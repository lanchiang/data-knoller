package de.hpi.isg.dataprep.model.target.errorlog;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class ErrorTypeProcessor<T> {

    private final static String VALUE_NOT_CONVERTIBLE = "value-not-convertible";

    public static String exceptionToErrorType(Throwable throwable) {
        if (throwable instanceof NumberFormatException) {
            return VALUE_NOT_CONVERTIBLE;
        }
        return "";
    }
}
