package de.hpi.isg.dataprep.exceptions;

/**
 * @author Lan Jiang
 * @since 2018/8/26
 */
public class EncodingNotSupportedException extends RuntimeException {
	
	private static final long serialVersionUID = -660506907039679878L;
	
	public EncodingNotSupportedException(String message) {
		super(message);
	}
}
