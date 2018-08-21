package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.MovePropertyImpl;
import de.hpi.isg.dataprep.model.metadata.MovePropertyMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * @author Lan Jiang
 * @since 2018/8/21
 */
public class MoveProperty extends Preparator {

    private String targetPropertyName;
    private int newPosition;

    public MoveProperty(MovePropertyImpl impl) {
        this.impl = impl;
        prerequisites = MovePropertyMetadata.getInstance();
    }

    @Override
    protected void recordProvenance() {

    }

    public String getTargetPropertyName() {
        return targetPropertyName;
    }

    public void setTargetPropertyName(String targetPropertyName) {
        this.targetPropertyName = targetPropertyName;
    }

    public int getNewPosition() {
        return newPosition;
    }

    public void setNewPosition(int newPosition) {
        this.newPosition = newPosition;
    }
}
