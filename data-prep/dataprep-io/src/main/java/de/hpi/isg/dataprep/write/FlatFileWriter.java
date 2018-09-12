package de.hpi.isg.dataprep.write;

import de.hpi.isg.dataprep.util.PrettyPrintable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/9/3
 */
public class FlatFileWriter<T extends PrettyPrintable> extends AbstractWriter<T> {

    // there should be a default output path
    private String separator = System.getProperty("file.separator");
    private String path = System.getProperty("user.dir") + separator + "pipelineError.txt";

//    private T content;

    public FlatFileWriter() {}

    public FlatFileWriter(String path) {
        this.path = path;
    }

    public void write(T content) throws IOException {
        List<String> prettyPrinted = content.getPrintedReady();
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(path));
            prettyPrinted.stream().forEachOrdered(recordPiece -> writeOneLine(recordPiece));
        } catch (IOException e) {
            throw e;
        } finally {
            bufferedWriter.close();
        }
    }
}
