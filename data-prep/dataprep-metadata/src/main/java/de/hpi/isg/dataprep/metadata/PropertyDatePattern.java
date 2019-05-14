package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;
import de.hpi.isg.dataprep.util.DatePattern;

/**
 * @author Lan Jiang
 * @since 2018/8/26
 */
public class PropertyDatePattern extends MetadataOld {

    private DatePattern.DatePatternEnum datePattern;

    private PropertyDatePattern() {
        super(PropertyDatePattern.class.getSimpleName());
    }

    public PropertyDatePattern(DatePattern.DatePatternEnum datePattern, MetadataScope scope) {
        this();
        this.scope = scope;
        this.datePattern = datePattern;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
        PropertyDatePattern that = (PropertyDatePattern) metadata;
        return this.datePattern.equals(that.datePattern);
    }
}
