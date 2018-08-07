package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.model.metadata.PrerequisiteMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.util.Executable;

import java.util.*;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class Preparator extends AbstractPreparator implements Executable {

    protected PrerequisiteMetadata prerequisites;
    private Preparation preparation;

    protected List<Metadata> invalidMetadata = null;

    public Preparation getPreparation() {
        return preparation;
    }

    public void setPreparation(Preparation preparation) {
        this.preparation = preparation;
    }
}