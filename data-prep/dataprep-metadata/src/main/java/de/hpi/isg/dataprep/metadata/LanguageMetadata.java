package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.ColumnMetadata;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;
import org.languagetool.Language;
import org.languagetool.language.*;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class LanguageMetadata extends Metadata {

    public enum LanguageEnum implements Serializable {
        ENGLISH(English.class),
        GERMAN(German.class),
        FRENCH(French.class),
        DUTCH(Dutch.class),
        SPANISH(Spanish.class),
        PORTUGUESE(Portuguese.class),
        RUSSIAN(Russian.class),
        CHINESE(Chinese.class);

        private final Class<? extends Language> type;
        LanguageEnum(Class<? extends Language> type) {
            this.type = type;
        }

        public Class<? extends Language> getType(){
            return this.type;
        }

        public static LanguageEnum langForClass(Class<?> type) throws UnsupportedLanguageException {
            for(LanguageEnum lang: values())
                if(lang.getType() == type)
                    return lang;
            throw new UnsupportedLanguageException("LanguageMetadata " + type.toString() + " not supported");
        }
    }

    private Map<Integer, LanguageEnum> languages;
    private int chunkSize = 5;

    public LanguageMetadata() {
        super(LanguageMetadata.class.getSimpleName());
    }

    public LanguageMetadata(String propertyName, Map<Integer, LanguageEnum> languages) {
        this();
        this.scope = new ColumnMetadata(propertyName);
        this.languages = languages;
    }

    public LanguageMetadata(String propertyName, Map<Integer, LanguageEnum> languages, int chunkSize) {
        this();
        this.scope = new ColumnMetadata(propertyName);
        this.languages = languages;
        this.chunkSize = chunkSize;
    }

    public MetadataScope getScope() {
        return scope;
    }

    public Map<Integer, LanguageEnum> getLanguages() {
        return languages;
    }

    public LanguageEnum getLanguage(int index){
        return languages.getOrDefault(index / chunkSize, null);
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
        List<LanguageMetadata> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof LanguageMetadata)
                .map(metadata -> (LanguageMetadata) metadata)
                .filter(metadata -> metadata.equals(this))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            throw new MetadataNotFoundException(String.format("Metadata %s not found in the repository.", this.toString()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Metadata %s has multiple data type for property: %s",
                    this.getClass().getSimpleName(), this.scope.getName()));
        } else {
            LanguageMetadata metadataInRepo = matchedInRepo.get(0);
            if (!this.equalsByValue(metadataInRepo)) {
                // value of this metadata does not match that in the repository.
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository."));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LanguageMetadata otherLang = (LanguageMetadata) o;
        return Objects.equals(scope, otherLang.getScope());
    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        if (!(metadata instanceof LanguageMetadata) || !this.equals(metadata))
            return false;
        Map<Integer, LanguageEnum> otherLangs = ((LanguageMetadata) metadata).getLanguages();
        if(languages == null || otherLangs == null) // ANY
            return true;
        return languages.equals(otherLangs);
    }

    @Override
    public String toString() {
        return "PropertyDataType{" +
                "propertyName='" + scope.getName() + '\'' +
                ", languages=" + new HashSet<LanguageEnum>(languages.values()).toString() +
                '}';
    }
}
