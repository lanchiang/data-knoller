package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;

/**
 * The delegation class for {@link ChangePropertyDataType}. Maybe not needed.
 *
 * @author Lan Jiang
 * @since 2018/8/9
 */
abstract public class ChangePropertyDataTypeImpl extends PreparatorImpl {

    protected ChangePropertyDataType getPreparatorInstance(Preparator preparator) throws ClassCastException {
        if (!(preparator instanceof ChangePropertyDataType)) {
            throw new ClassCastException("Class is not the required type.");
        }
        ChangePropertyDataType realPreparator = (ChangePropertyDataType) preparator;
        return realPreparator;
    }
}
