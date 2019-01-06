package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * Each comment starts with commentcharacter
 *
 * @author Lasse Kohlmeyer
 * @since 2018/11/28
 */
public class CommentCharacter extends Metadata {

    private String commentCharacter;

    private CommentCharacter() {
        super(CommentCharacter.class.getSimpleName());
    }

    public CommentCharacter(String commentCharacter, MetadataScope scope) {
        this();
        this.scope = scope;
        this.commentCharacter = commentCharacter;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public String getCommentCharacter() {
        return commentCharacter;
    }
}
