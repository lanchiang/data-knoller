package de.hpi.isg.dataprep.write;

import java.io.BufferedWriter;
import java.io.IOException;

/**
 * @author Lan Jiang
 * @since 2018/9/3
 */
abstract public class AbstractWriter<T> {

    protected BufferedWriter bufferedWriter;

    public void writeOneLine(String line) {
        try {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
