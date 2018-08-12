package de.hpi.isg.dataprep.implementation.abstracts;

import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;

/**
 * The delegation class for {@link ChangePropertyDataType}.
 *
 * @author Lan Jiang
 * @since 2018/8/9
 */
abstract public class ChangePropertyDataTypeImpl {

    abstract public void execute(ChangePropertyDataType preparator) throws Exception;
}
