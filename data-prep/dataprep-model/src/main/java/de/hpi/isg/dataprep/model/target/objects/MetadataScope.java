package de.hpi.isg.dataprep.model.target.objects;

import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.util.Nameable;
import scala.Serializable;

/**
 * Super class of all the operated objects for preparators, such as a property, or a whole dataset.
 *
 * @author Lan Jiang
 * @since 2018/9/2
 */
abstract public class MetadataScope extends Target implements Nameable, Serializable {

}
