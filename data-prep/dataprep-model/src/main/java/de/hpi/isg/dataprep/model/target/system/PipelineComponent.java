package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.util.Nameable;

/**
 * This class is the superclass for both {@link Pipeline} and {@link Preparation}. It represents these two units in a data preparation pipeline.
 *
 * @author Lan Jiang
 * @since 2018/8/9
 */
abstract public class PipelineComponent extends Target implements Nameable {
}
