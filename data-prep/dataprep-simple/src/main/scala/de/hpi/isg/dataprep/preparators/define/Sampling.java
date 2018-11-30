package de.hpi.isg.dataprep.preparators.define;

import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;

import java.io.Serializable;

public class Sampling extends Preparator implements Serializable {


    public double getPercentage() {
        return percentage;
    }

    private double percentage;

    public void setPercentage(double percentage)
    {
        this.percentage = percentage;
    }

    public Sampling(double percentage) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        super();
        this.percentage = percentage;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException  {

    }
}
