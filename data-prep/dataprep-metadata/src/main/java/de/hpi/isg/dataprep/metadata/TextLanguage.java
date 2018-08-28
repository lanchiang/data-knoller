package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class TextLanguage extends Metadata {

    private final String name = "text-language";

    private String propertyName;
    private TextLanguage textLanguage;

    public TextLanguage(String propertyName, TextLanguage textLanguage) {
        this.propertyName = propertyName;
        this.textLanguage = textLanguage;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getPropertyName() {
        return propertyName;
    }

    public TextLanguage getTextLanguage() {
        return textLanguage;
    }

    @Override
    public String getName() {
        return name;
    }
}
