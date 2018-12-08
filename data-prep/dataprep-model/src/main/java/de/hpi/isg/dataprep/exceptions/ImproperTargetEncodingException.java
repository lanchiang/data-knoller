package de.hpi.isg.dataprep.exceptions;

import java.io.IOException;
import java.nio.charset.Charset;

public class ImproperTargetEncodingException extends IOException {

    public ImproperTargetEncodingException(String string, Charset charset) {
        super(String.format("Encoding %s can not encode the string '%s'", charset.name(), string));
    }
}
