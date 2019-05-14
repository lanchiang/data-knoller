package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;

import java.util.Objects;

/**
 * @author Lukas Behrendt, Oliver Clasen, Lisa Ihde
 * @since 2019/01/19
 */
public class CSVSourcePath extends MetadataOld {
    private String csvSourcePath;

    private CSVSourcePath() {
        super(CSVSourcePath.class.getSimpleName());
    }

    public CSVSourcePath(String path) {
        this();
        csvSourcePath = path;
    }

    private String getCsvSourcePath() {
        return csvSourcePath;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
//        List<CSVSourcePath> matchedInRepo = metadataRepository.getMetadataPool().stream()
//                .filter(metadata -> metadata instanceof CSVSourcePath)
//                .map(metadata -> (CSVSourcePath) metadata)
//                .collect(Collectors.toList());
//
//        // the metadataRepository should hold exactly one CSVSourcePath, which should be the same as specified in this
//        if (matchedInRepo.size() == 0) {
//            throw new MetadataNotFoundException(String.format("MetadataOld %s not found in the repository.", getClass().getSimpleName()));
//        } else if (matchedInRepo.size() > 1) {
//            throw new DuplicateMetadataException(String.format("Multiple pieces of metadata %s found in the repository.", getClass().getSimpleName()));
//        } else {
//            CSVSourcePath metadataInRepo = matchedInRepo.get(0);
//            if (!getCsvSourcePath().equals(metadataInRepo.getCsvSourcePath())) {
//                throw new MetadataNotMatchException("MetadataOld value does not match that in the repository.");
//            }
//        }
    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
        if (metadata instanceof CSVSourcePath) {
            return this.csvSourcePath.equals(((CSVSourcePath)metadata).csvSourcePath);
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return Objects.hash(csvSourcePath);
    }

    public String getPath() {
        return csvSourcePath;
    }
}
