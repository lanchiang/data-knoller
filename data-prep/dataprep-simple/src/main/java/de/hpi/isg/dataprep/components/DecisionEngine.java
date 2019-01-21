package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.iterator.SubsetIterator;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.model.target.system.Engine;
import de.hpi.isg.dataprep.utility.ClassUtility;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The decision engine makes decisions such as selecting most suitable preparator for recommendation during the preparation
 * execution period.
 *
 * @author lan.jiang
 * @since 12/17/18
 */
public class DecisionEngine implements Engine {

    private final static int MAX_ITERATION = 100;
    private int iteration_count = 0;

    private final static String PREPARATOR_PACKAGE_PATH = "de.hpi.isg.dataprep.preparators.define";

    private static DecisionEngine instance;

    private AbstractPipeline pipeline;

    /**
     * specifies the preparator candidates that the decision engine may call. Could be moved to the controller.
     */
//    private final static String[] preparatorCandidates = {"SplitProperty", "MergeProperty", "ChangeDateFormat", "RemovePreamble",
//            "ChangePhoneFormat", "ChangeEncoding", "StemPreparator"};
    private final static String[] preparatorCandidates = {"AddProperty", "Collapse"};

    private Set<AbstractPreparator> preparators;
    private Map<AbstractPreparator, Float> scores;

    private DecisionEngine() {
    }

    // get the instance of the class only by this method
    public static DecisionEngine getInstance() {
        if (instance == null) {
            instance = new DecisionEngine();
        }
        return instance;
    }

    /**
     * Instantiate the concrete preparators that are used in the select best preparator method.
     */
    private void initDecisionEngine() {
        ClassUtility.checkClassesExistence(preparatorCandidates, PREPARATOR_PACKAGE_PATH);

        scores = new HashMap<>();
        preparators = new HashSet<>();

        for (String string : preparatorCandidates) {
            Class clazz;
            try {
                clazz = Class.forName(ClassUtility.getPackagePath(string, PREPARATOR_PACKAGE_PATH));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class cannot be found: " + string, e);
            }
            Class<? extends AbstractPreparator> concreteClazz = (Class<? extends AbstractPreparator>) clazz;
            try {
                preparators.add(AbstractPreparator.getPreparatorInstance(concreteClazz));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the most suitable parameterized preparator recommended for the current step in the pipeline according to the applicability scores
     * of all the candidate preparators.
     *
     * @param pipeline the pipeline on which the decision engine process
     *
     * @return the most suitable parameterized preparator. Returning null indicates the termination of the process
     */
//    public AbstractPreparator selectBestPreparator(Dataset<Row> dataset, Collection<Metadata> targetMetadata) {
    public AbstractPreparator selectBestPreparator(AbstractPipeline pipeline) {
        if (stopProcess()) {
            return null;
        }

        // every time this method is called, instantiate all the preparator candidates again.
        initDecisionEngine();

        Dataset<Row> dataset = pipeline.getRawData();
        // first the column combinations need to be generated.
        StructField[] fields = dataset.schema().fields();
        List<String> fieldName = Arrays.stream(fields).map(field -> field.name()).collect(Collectors.toList());

        // Todo: this is wrong now. It is not target metadata, but current ones. Put here as a placeholder to allow compilation.
        Collection<Metadata> targetMetadata = pipeline.getMetadataRepository().getMetadataPool();
        // using this permutation iterator cannot specify the maximal number of columns.
        SubsetIterator<String> iterator = new SubsetIterator<>(fieldName, 5);
        while (iterator.hasNext()) {
            List<String> colNameCombination = iterator.next();
            List<Column> columns = colNameCombination.stream().map(colName -> new Column(colName)).collect(Collectors.toList());
            Column[] columnArr = new Column[columns.size()];
            columnArr = columns.toArray(columnArr);

//            Column[] columnArr = colNameCombination.stream().toArray(Column[]::new);

            Dataset<Row> dataSlice = dataset.select(columnArr);
            for (AbstractPreparator preparator : preparators) {
                float score = preparator.calApplicability(null, dataSlice, targetMetadata);
                // the same preparator: only with the same class name
                float currentScore = scores.getOrDefault(preparator, Float.MIN_VALUE);
                if (currentScore==Float.MIN_VALUE) {
                    scores.put(preparator, score);
                }
                else {
                    if (score > currentScore) {
                        scores.put(preparator, score);
                    }
                }
            }
        }

        // Find the preparator with the highest score.
        float highest = Float.MIN_VALUE;
        AbstractPreparator bestPreparator = null;
        Iterator<Map.Entry<AbstractPreparator, Float>> entryIterator = scores.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<AbstractPreparator, Float> next = entryIterator.next();
            if (next.getValue() > highest) {
                bestPreparator = next.getKey();
                highest = next.getValue();
            }
        }

        iteration_count++;
        return bestPreparator;
    }

    /**
     * When one of the termination conditions is met, return true to tell the decision engine stop the automation process.
     *
     * @return true if one of the termination conditions is met, otherwise false.
     */
    private boolean stopProcess() {
        if (iteration_count == MAX_ITERATION) {
            return true;
        }
        return false;
    }

    // Todo: the decision engine needs to notify the pipeline that the dataset needs to be updated, after executing a recommended preparator.
}
