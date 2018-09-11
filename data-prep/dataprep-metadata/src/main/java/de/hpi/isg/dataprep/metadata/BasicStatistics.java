package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

import java.util.Set;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class BasicStatistics extends Metadata {

    private final String name = "basic-statistics";

    private String propertyName;
    private Set<String> basicStatistics;

    public BasicStatistics(String propertyName, Set<String> basicStatistics) {
        this.propertyName = propertyName;
        this.basicStatistics = basicStatistics;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return propertyName;
    }

    public Set<String> getBasicStatistics() {
        return basicStatistics;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getName() {
        return name;
    }
}
