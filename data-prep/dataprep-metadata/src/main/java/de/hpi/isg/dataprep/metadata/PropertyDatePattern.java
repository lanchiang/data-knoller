package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;
import de.hpi.isg.dataprep.util.DatePattern;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/26
 */
public class PropertyDatePattern extends Metadata {

    private final String name = "property-date-pattern";

    private DatePattern.DatePatternEnum datePattern;

    public PropertyDatePattern(MetadataScope scope, DatePattern.DatePatternEnum datePattern) {
        this.scope = scope;
        this.datePattern = datePattern;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        PropertyDatePattern that = (PropertyDatePattern) metadata;
        return this.datePattern.equals(that.datePattern);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyDatePattern that = (PropertyDatePattern) o;
        return scope == that.scope;
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope);
    }
}
