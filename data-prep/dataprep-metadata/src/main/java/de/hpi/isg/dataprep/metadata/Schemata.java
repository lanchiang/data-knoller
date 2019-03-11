package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.TableMetadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;

import java.util.List;

/**
 * This metadata describes the schema of a dataset.
 *
 * @author lan.jiang
 * @since 1/28/19
 */
public class Schemata extends Metadata {

    private List<Attribute> attributes;

    public Schemata(String tableName, List<Attribute> attributes) {
        super(Schemata.class.getSimpleName());
        this.attributes = attributes;
        this.scope = new TableMetadata(tableName);
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        if(metadata instanceof Schemata) {
            long validCount = ((Schemata)metadata).attributes.stream().filter(attribute -> this.attributes.contains(attribute)).count();
            return validCount == attributes.size();
        }
        return false;
    }
}
