package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

import java.util.Objects;

/**
 * A pairwise string identifier wraps the position of real data content in a data file.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class QuoteCharacter extends Metadata {

    private final String name = "quote";

    private String QuoteCharacter;

    public QuoteCharacter(String QuoteCharacter) {
        this.QuoteCharacter = QuoteCharacter;
    }

    public QuoteCharacter(MetadataScope scope, String quoteCharacter) {
        this.scope = scope;
        QuoteCharacter = quoteCharacter;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public String getQuoteCharacter() {
        return QuoteCharacter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuoteCharacter that = (QuoteCharacter) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(QuoteCharacter, that.QuoteCharacter);
    }

    @Override
    public int hashCode() {

        return Objects.hash(scope, QuoteCharacter);
    }
}
