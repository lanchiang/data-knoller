package de.hpi.isg.dataprep.model.objects.metadata;

import java.util.Objects;

/**
 * @author Lasse Kohlmeyer
 * @since 2018/12/3
 */
public class FileMetadata extends MetadataScope {

    private String filepath;

    public FileMetadata(String filepath) {
        this.filepath = filepath;
    }

    public String getFilepath() {
        return filepath;
    }

    @Override
    public String getName() {
        return filepath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata fileMetadata = (FileMetadata) o;
        return Objects.equals(filepath, fileMetadata.filepath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filepath);
    }
}
