package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;

import java.util.Objects;

/**
 * @author Lukas Behrendt, Oliver Clasen, Lisa Ihde
 * @since 2019/01/19
 */
public class UsedEncoding extends MetadataOld {
    private String usedEncoding;

    private UsedEncoding() {
        super(UsedEncoding.class.getSimpleName());
    }

    public UsedEncoding(String encoding) {
        this();
        usedEncoding = encoding;
    }

    public String getUsedEncoding() {
        return usedEncoding;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
//        List<UsedEncoding> matchedInRepo = metadataRepository.getMetadataPool().stream()
//                .filter(metadata -> metadata instanceof UsedEncoding)
//                .map(metadata -> (UsedEncoding) metadata)
//                .collect(Collectors.toList());
//
//        if (matchedInRepo.size() == 0) {
//            throw new MetadataNotFoundException(String.format("MetadataOld %s not found in the repository.", getClass().getSimpleName()));
//        } else if (matchedInRepo.size() > 1) {
//            throw new DuplicateMetadataException(String.format("Multiple pieces of metadata %s found in the repository.", getClass().getSimpleName()));
//        } else {
//            UsedEncoding metadataInRepo = matchedInRepo.get(0);
//            if (!getUsedEncoding().equals(metadataInRepo.getUsedEncoding())) {
//                throw new MetadataNotMatchException("MetadataOld value does not match that in the repository.");
//            }
//        }
    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
        if (metadata instanceof UsedEncoding) {
            return this.usedEncoding.equals(((UsedEncoding)metadata).getUsedEncoding());
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return Objects.hash(usedEncoding);
    }
}
