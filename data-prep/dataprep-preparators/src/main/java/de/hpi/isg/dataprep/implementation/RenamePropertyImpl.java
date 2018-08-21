package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.RenameProperty;

/**
 * @author Lan Jiang
 * @since 2018/8/17
 */
abstract public class RenamePropertyImpl extends PreparatorImpl {

    protected RenameProperty getPreparatorInstance(Preparator preparator) throws ClassCastException {
        if (!(preparator instanceof RenameProperty)) {
            throw new ClassCastException("Class is not the required type.");
        }
        RenameProperty realPreparator = (RenameProperty) preparator;
        return realPreparator;
    }
}
