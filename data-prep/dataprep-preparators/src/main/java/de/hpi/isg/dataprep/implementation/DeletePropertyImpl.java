package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.DeleteProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
abstract public class DeletePropertyImpl extends PreparatorImpl {

    protected DeleteProperty getPreparatorInstance(Preparator preparator) throws ClassCastException {
        if (!(preparator instanceof DeleteProperty)) {
            throw new ClassCastException("Class is not the required type.");
        }
        DeleteProperty realPreparator = (DeleteProperty) preparator;
        return realPreparator;
    }
}
