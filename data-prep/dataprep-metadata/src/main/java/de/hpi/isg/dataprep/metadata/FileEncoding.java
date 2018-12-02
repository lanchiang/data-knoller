package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
 * @since 2018/10/29
 */
public class FileEncoding extends Metadata {

    private static final long serialVersionUID = -5529216369913096740L;
    private String fileName;
    private Charset fileEncoding;

    private FileEncoding() {
        super(FileEncoding.class.getSimpleName());
    }

    public FileEncoding(String fileName, Charset fileEncoding) {
        this();
        this.fileName = fileName;
        this.fileEncoding = fileEncoding;

    }

    public String getFileName() {
        return fileName;
    }

    public Charset getFileEncoding() {
        return fileEncoding;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
        List<FileEncoding> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof FileEncoding)
                .map(metadata -> (FileEncoding) metadata)
                .filter(metadata -> metadata.getFileName().equals(fileName))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            throw new MetadataNotFoundException(String.format("Metadata %s not found in the repository.", getClass().getSimpleName()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Multiple pieces of metadata %s found in the repository.", getClass().getSimpleName()));
        } else {
            FileEncoding metadataInRepo = matchedInRepo.get(0);
            if (!getFileEncoding().equals(metadataInRepo.getFileEncoding())) {
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository."));
            }
        }
    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return false;
    }

    @Override
    public String getName() {
        return scope.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileEncoding that = (FileEncoding) o;
        return Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName);
    }

    @Override
    public String toString() {
        return "FileEncoding{" +
                "fileName='" + fileName + '\'' +
                ", fileEncoding=" + fileEncoding +
                '}';
    }
}
