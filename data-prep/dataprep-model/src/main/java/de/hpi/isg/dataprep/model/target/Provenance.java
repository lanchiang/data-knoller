package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Provenance extends Target {

    private Preparation preparation;
    private Preparator preparator;
    private String createdTime;
    private String messages;
}
