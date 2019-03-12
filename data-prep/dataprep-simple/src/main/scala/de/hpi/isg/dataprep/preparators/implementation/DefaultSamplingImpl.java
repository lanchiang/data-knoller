package de.hpi.isg.dataprep.preparators.implementation;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.preparators.define.Sampling;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

import java.util.Random;

import static org.apache.spark.sql.functions.*;

public class DefaultSamplingImpl extends AbstractPreparatorImpl {


    /**
     * Select which sampling-method to use based on used api / given input parameters.
     *
     * @param abstractPreparator is the instance of {@link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
     * @param dataFrame          contains the intermediate dataset
     * @param errorAccumulator   is the {@link CollectionAccumulator} to store preparation errors while executing the preparator.
     * @return
     * @throws Exception
     */
    @Override
    protected ExecutionContext executeLogic(AbstractPreparator abstractPreparator, Dataset<Row> dataFrame, CollectionAccumulator<PreparationError> errorAccumulator) throws Exception {

        Sampling sampler = (Sampling) abstractPreparator;
        if (sampler.getProbability() > 0)
            //return new ExecutionContext(simpleRandomSampling(dataFrame,sampler.getProbability()),errorAccumulator);
            return new ExecutionContext(dataFrame.sample(sampler.isWithReplacement(), sampler.getProbability()), errorAccumulator);
        if (sampler.getTargetRecordCount() > 0)
            return new ExecutionContext(shuffleSampling(dataFrame, (int) sampler.getTargetRecordCount()), errorAccumulator);
        //return new ExecutionContext(reservoirSampling(dataFrame,(int)sampler.getTargetRecordCount()),errorAccumulator);
        throw new Exception("None of the provided apis were used!");
    }

    /**
     * A simple function to do ramdom sampling. Returend Sample is not guarantee to have probability * len(Dataset) size.
     *
     * @param dataFrame
     * @param probability
     * @return
     */
    private Dataset<Row> simpleRandomSampling(Dataset<Row> dataFrame, double probability) {
        Random rand = new Random();
        return dataFrame.filter((Row row) -> rand.nextDouble() < probability);
    }

    /**
     * Shuffle the rows in dataFrame then select top n rows.
     *
     * @param dataFrame  DataSet to sample from
     * @param sampleSize guaranteed sample size
     * @return Sample as a new DataSet
     */
    private Dataset<Row> shuffleSampling(Dataset<Row> dataFrame, int sampleSize) {
        return dataFrame.sort(rand()).limit(sampleSize);
    }

    /**
     * reservoirSampling unfortunatly doesnt work atm
     *
     * @param dataFrame
     * @param sampleSize
     * @return
     */
//    private Dataset<Row> reservoirSampling(Dataset<Row> dataFrame, int sampleSize) {
//
//        List<Row> result = new ArrayList<>();
//        Random random = new Random();
//        int index = 0;
//        dataFrame.foreach(
//                (Row row) ->
//                {
//                    if (result.size() < sampleSize) {
//                        result.add(row.copy());
//                    } else {
//                        double prob = ((double) sampleSize) / index;
//                        Boolean keep = random.nextDouble() < prob;
//                        if (keep) {
//                            int replaceIndex = random.nextInt(sampleSize);
//                            result.set(replaceIndex, row);
//                        }
//                    }
//                    index++;
//                }
//        );
//        return SparkSession.getActiveSession().get().createDataFrame(result, dataFrame.schema());
//    }
}
