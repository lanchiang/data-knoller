package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.SplitAttribute;
import org.junit.Test;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class AddRecommendedPreparatorTest {

    @Test
    public void getPreparatorInstanceTest() throws InstantiationException, IllegalAccessException {
        AbstractPreparator preparator = AbstractPreparator.getPreparatorInstance(SplitAttribute.class);
        preparator.calApplicability(null, null, null);
    }
}
