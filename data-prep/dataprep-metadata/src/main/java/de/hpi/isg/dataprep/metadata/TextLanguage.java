package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.cases.Language;
import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class TextLanguage extends Metadata {

    private final String name = "text-language";

    private String propertyName;
    private Language textLanguage;

    public TextLanguage(String propertyName, Language textLanguage) {
        this.propertyName = propertyName;
        this.textLanguage = textLanguage;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
        // try to get text language of current property from the metadata repository.
        List<TextLanguage> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof TextLanguage)
                .map(metadata -> (TextLanguage) metadata)
                .filter(metadata -> metadata.getPropertyName().equals(this.propertyName))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            throw new MetadataNotFoundException(String.format("The metadata %s not found in the repository", this.toString()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Metadata %s has multiple data type for property: %s",
                    this.getClass().getSimpleName(), this.propertyName));
        } else {
            TextLanguage metadataInRepo = matchedInRepo.get(0);
            if (!this.textLanguage.equals(metadataInRepo.getTextLanguage())) {
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository"));
            }
        }
    }

    @Override
    public String getTargetName() {
        return propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Language getTextLanguage() {
        return textLanguage;
    }

    @Override
    public String getName() {
        return name;
    }
}
