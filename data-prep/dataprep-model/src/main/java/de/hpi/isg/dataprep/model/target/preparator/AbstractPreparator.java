package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.util.Executable;

/**
 * The template class for the execution of a preparator.
 *
 * @author Lan Jiang
 * @since 2018/8/7
 */
abstract public class AbstractPreparator extends Target implements Executable {

    /**
     * Checks whether the prerequisite metadata are met.
     *
     * @return true/false if all the prerequisites are/are not met.
     */
    public abstract boolean checkMetadata();

    /**
     * The execution of the preparator.
     *
     */
    public abstract void executePreparator();

    /**
     * Call this method whenever an error occurs during the preparator execution in order to
     * record an error log.
     */
    public abstract void recordErrorLog();

    /**
     * After the preparator execution succeeds, call this method to record the provenance.
     */
    public abstract void recordProvenance();

    @Override
    public void execute() throws Exception {
        if (!checkMetadata()) {
            throw new Exception("The prerequisite metadata are not met, check first.");
        }
        try {
            executePreparator();
        } catch (Exception e) {
            recordErrorLog();
        }
        recordProvenance();
    }
}
