package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.MoveProperty;

/**
 * @author Lan Jiang
 * @since 2018/8/21
 */
abstract public class MovePropertyImpl extends PreparatorImpl {

    protected MoveProperty getPreparatorInstance(Preparator preparator) throws ClassCastException {
        if (!(preparator instanceof MoveProperty)) {
            throw new ClassCastException("Class is not required type.");
        }
        MoveProperty realPreparator = (MoveProperty) preparator;
        return realPreparator;
    }
}
