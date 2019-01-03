package de.hpi.isg.dataprep.model.target.system;

/**
 * The decision engine makes decisions such as selecting most suitable preparator for recommendation during the preparation
 * execution period.
 *
 * @author lan.jiang
 * @since 12/17/18
 */
public class DecisionEngine {

    private static DecisionEngine instance;

    // private avoid being initialized
    private DecisionEngine() {}

    // get the instance of the class only by this method
    public DecisionEngine getInstance() {
        if (instance == null) {
            instance = new DecisionEngine();
        }
        return instance;
    }

    /**
     * Returns the most suitable parameterized preparator recommended for the current step
     * in the pipeline according to the applicability scores of all the candidate preparators.
     * @return the most suitable parameterized preparator.
     */
    AbstractPreparator selectBestPreparator() {
        return null;
    }
}
