package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Provenance extends Target {

    private AbstractPreparation preparation;
    private AbstractPreparator preparator;
    private String createdTime;
    private String messages;
}
