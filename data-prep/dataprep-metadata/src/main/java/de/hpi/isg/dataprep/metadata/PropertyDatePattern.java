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

    private DatePattern.DatePatternEnum datePattern;

    private PropertyDatePattern() {
        super("property-date-pattern");
    }

    public PropertyDatePattern(MetadataScope scope, DatePattern.DatePatternEnum datePattern) {
        this();
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
}
