package de.hpi.isg.dataprep.write;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

/**
 * @author Lan Jiang
 * @since 2018/9/3
 */
public class FlatFileWriter<T> extends AbstractWriter<T> {

    // there should be a default output path
    private String separator = System.getProperty("file.separator");
    private String path = System.getProperty("user.dir") + separator + "pipelineError.txt";

    private Collection<T> content;

    public FlatFileWriter(Collection<T> content) {
        this.content = content;
    }

    public FlatFileWriter(Collection<T> content, String path) {
        this(content);
        this.path = path;
    }

    public void write() throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
        for (T piece : content) {
            bufferedWriter.write(piece.toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();
    }
}
