package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.AddProperty;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
abstract public class AddPropertyImpl extends PreparatorImpl {

    protected AddProperty getPreparatorInstance(Preparator preparator) throws ClassCastException {
        if (!(preparator instanceof AddProperty)) {
            throw new ClassCastException("Class is not the required type.");
        }
        AddProperty realPreparator = (AddProperty) preparator;
        return realPreparator;
    }
}
