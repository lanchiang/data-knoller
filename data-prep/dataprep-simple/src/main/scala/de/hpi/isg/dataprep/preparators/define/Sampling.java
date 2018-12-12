package de.hpi.isg.dataprep.preparators.define;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;

import java.io.Serializable;

public class Sampling extends AbstractPreparator implements Serializable {

    private boolean withReplacement;
    private long targetRecordCount;
    private String dist;
    private double probability;


    public String getDist() {
        return dist;
    }

    public boolean isWithReplacement() {
        return withReplacement;
    }

    public double getProbability() {
        return probability;
    }

    public long getTargetRecordCount() {
        return targetRecordCount;
    }


    public Sampling(long targetRecordCount, boolean withReplacement) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        super();
        this.targetRecordCount = targetRecordCount;
        this.withReplacement = withReplacement;
    }

    public Sampling(double probability, boolean withReplacement) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        super();
        this.withReplacement = withReplacement;
        this.probability = probability;

    }

    //    public Sampling(long expectCount, boolean withReplacement) throws ClassNotFoundException, IllegalAccessException, InstantiationException
//    {
//        super();
//        this.withReplacement = withReplacement;
//        this.expectCount = expectCount;
//
//
//    }
    public Sampling(String dist, boolean withReplacement) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.dist = dist;
        this.withReplacement = withReplacement;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {

    }
}
