package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class FileEncoding extends MetadataOld {

    private static final long serialVersionUID = -5529216369913096740L;
    private String propertyName;
    private String fileEncoding;

    private FileEncoding() {
        super(FileEncoding.class.getSimpleName());
    }

    public FileEncoding(String propertyName, String fileEncoding) {
        this();
        this.propertyName = propertyName;
        this.fileEncoding = fileEncoding;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getFileEncoding() {
        return fileEncoding;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
//        List<FileEncoding> matchedInRepo = metadataRepository.getMetadataPool().stream()
//                .filter(metadata -> metadata instanceof FileEncoding)
//                .map(metadata -> (FileEncoding) metadata)
//                .filter(metadata -> metadata.getPropertyName().equals(propertyName))
//                .collect(Collectors.toList());
//
//        if (matchedInRepo.size() == 0) {
//            throw new MetadataNotFoundException(String.format("MetadataOld %s not found in the repository.", getClass().getSimpleName()));
//        } else if (matchedInRepo.size() > 1) {
//            throw new DuplicateMetadataException(String.format("Multiple pieces of metadata %s found in the repository.", getClass().getSimpleName()));
//        } else {
//            FileEncoding metadataInRepo = matchedInRepo.get(0);
//            if (!getFileEncoding().equals(metadataInRepo.getFileEncoding())) {
//                throw new MetadataNotMatchException(String.format("MetadataOld value does not match that in the repository."));
//            }
//        }
    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileEncoding that = (FileEncoding) o;
        return Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyName);
    }

    @Override
    public String toString() {
        return "FileEncoding{" +
                "propertyName='" + propertyName + '\'' +
                ", fileEncoding=" + fileEncoding +
                '}';
    }
}
