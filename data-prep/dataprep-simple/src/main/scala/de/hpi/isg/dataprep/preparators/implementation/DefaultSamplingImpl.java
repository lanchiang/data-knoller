package de.hpi.isg.dataprep.preparators.implementation;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.components.PreparatorImpl;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.Sampling;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

import java.util.Random;
import java.util.stream.IntStream;

public class DefaultSamplingImpl extends PreparatorImpl {

    @Override
    protected ExecutionContext executeLogic(AbstractPreparator abstractPreparator, Dataset<Row> dataFrame, CollectionAccumulator<PreparationError> errorAccumulator) throws Exception {

        Sampling sampler = (Sampling) abstractPreparator;
        Random rand = new Random();
        return new ExecutionContext(dataFrame.filter((Row x) -> rand.nextInt(100) < sampler.getPercentage() *100 ),errorAccumulator);
    }
}
