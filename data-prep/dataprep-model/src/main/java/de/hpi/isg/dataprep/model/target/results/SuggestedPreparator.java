package de.hpi.isg.dataprep.model.target.results;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;

/**
 * The wrapper of a preparator suggested by the decision engine. It contains the parameterized preparator, and its applicability score.
 *
 * @author Lan Jiang
 * @since 2019-05-07
 */
public class SuggestedPreparator {

    /**
     * The suggested parameterized preparator.
     */
    private AbstractPreparator preparator;

    /**
     * The score that such a preparator obtains when suggested.
     */
    private float score;

    public SuggestedPreparator(AbstractPreparator preparator, float score) {
        this.preparator = preparator;
        this.score = score;
    }

    public float getScore() {
        return score;
    }

    public AbstractPreparator getPreparator() {
        return preparator;
    }

    @Override
    public String toString() {
        return "SuggestedPreparator{" +
                "preparator=" + preparator +
                ", score=" + score +
                '}';
    }
}
