package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.iterator.SubsetIterator;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
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
 * The decision engine makes decisions such as selecting most suitable preparator for recommendation during the preparation execution period.
 *
 * @author lan.jiang
 * @since 12/17/18
 */
public class DecisionEngine implements Engine {

    /**
     * The maximum number of iteration times before the forced termination of the suggestion process
     */
    private final static int MAX_ITERATION = 10;

    /**
     * The maximum cardinality of the column combinations
     */
    private final static int MAX_COLUMN_COMBINATION_SIZE = 2;

    /**
     * The package path of the preparator classes
     */
    private final static String PREPARATOR_PACKAGE_PATH = "de.hpi.isg.dataprep.preparators.define";

    /**
     * maybe the decision engine should not include this count, because this is pipeline-specific, should be moved to the pipeline.
     */
    private int iteration_count = 0;

    /**
     * The size of the suggested preparator list.
     */
    private int suggested_list_size = 1;

    /**
     * The singular instance of {@link DecisionEngine}
     */
    private static DecisionEngine instance;

    /**
     * The preparator candidates that may be suggested by the decision engine.
     * This may be moved to the controller.
     */
    private static String[] preparatorCandidates = {"ChangeDateFormat", "ChangeFilesEncoding","DeleteProperty",
            "DetectLanguagePreparator", "LemmatizePreparator", "MergeAttribute", "RemovePreamble", "SplitProperty"};

    /**
     * The preparator instances whose calApplicability functions are called while searching for the best preparator.
     * Note that only the preparators specified in the {@link #preparatorCandidates} variable can be instantiated.
     * The instantiation is performed by java reflection that calls the default parameter-free constructors of them.
     * The parameters of each preparator instance will be filled later by the calApplicability functions.
     */
    private Set<AbstractPreparator> preparators;

    /**
     * This vector is used to record the applicability score of all the preparator candidates.
     * The key is a parameterized preparator instance, whereas the value is the applicability score this preparator gets.
     */
    private Map<AbstractPreparator, Float> scores;

    private DecisionEngine() {}

    // the whole runtime needs only one decision engine instance
    public static DecisionEngine getInstance() {
        if (instance == null) {
            instance = new DecisionEngine();
        }
        return instance;
    }

    /**
     * Instantiate the concrete preparators that are used in the select best preparator method by reflection.
     */
    private void initDecisionEngine() {
        ClassUtility.checkClassesExistence(preparatorCandidates, PREPARATOR_PACKAGE_PATH);
        instantiatePreparator();

        this.scores = new HashMap<>();
    }

    /**
     * Instantiate the preparator classes with their default constructor by using java reflection.
     */
    private void instantiatePreparator() {
        preparators = new HashSet<>();
        for (String string : preparatorCandidates) {
            Class clazz;
            try {
                clazz = Class.forName(ClassUtility.getClassFullPath(string, PREPARATOR_PACKAGE_PATH));
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
     * Check whether pipeline has been properly pre-configured.
     *
     * @param pipeline is the pipeline instance to be checked.
     */
    private void checkPipelineConfiguration(AbstractPipeline pipeline) {
        SchemaMapping schemaMapping = pipeline.getSchemaMapping();
        Set<Metadata> targetMetadata = pipeline.getTargetMetadata();

        if (schemaMapping == null) {
            throw new RuntimeException(new NullPointerException("Schema mapping instance not found."));
        }
        if (targetMetadata == null) {
            throw new RuntimeException(new NullPointerException("Target metadata set instance not found."));
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
    public AbstractPreparator selectBestPreparator(AbstractPipeline pipeline) {
        checkPipelineConfiguration(pipeline);
        SchemaMapping schemaMapping = pipeline.getSchemaMapping();
        Set<Metadata> targetMetadata = pipeline.getTargetMetadata();

        // every time this method is called, instantiate all the preparator candidates again.
        initDecisionEngine();

        Dataset<Row> dataset = pipeline.getDataset();
        // first the column combinations need to be generated.
        StructField[] fields = dataset.schema().fields();
        List<String> fieldName = Arrays.asList(dataset.schema().fieldNames());

        if (fields.length == 0) {
            // return null if the dataset is empty.
            return null;
        }

        // using this permutation iterator cannot specify the maximal number of columns.
        SubsetIterator<String> iterator = new SubsetIterator<>(fieldName, MAX_COLUMN_COMBINATION_SIZE);
        while (iterator.hasNext()) {
            List<String> colNameCombination = iterator.next();

            // do not consider the column combination with no columns.
            if (colNameCombination.size()==0) {
                continue;
            }

            // create a new DataFrame including the columns specified by the column combination
            // Todo: for file-level and table-level preparators, e.g., remove preamble, there is no need to calculate the score for each column combination
            List<Column> columns = colNameCombination.stream().map(colName -> new Column(colName)).collect(Collectors.toList());
            Column[] columnArr = new Column[columns.size()];
            columnArr = columns.toArray(columnArr);
            Dataset<Row> dataSlice = dataset.select(columnArr);

            for (AbstractPreparator preparator : preparators) {
                float score = preparator.calApplicability(schemaMapping, dataSlice, targetMetadata);
                // the same preparator: only with the same class name
                float currentScore = scores.getOrDefault(preparator, Float.MIN_VALUE);
                if (currentScore==Float.MIN_VALUE) {
                    scores.put(preparator, score);
                }
                else {
                    if (score > currentScore) {
                        // replace the preparator instance in the key with the new preparator instance, though their names are the same.
                        scores.remove(preparator);
                        scores.put(preparator, score);
                    }
                }
            }

            // create new empty preparator instances that will be filled by parameters.
            instantiatePreparator();
        }

        // Find the preparator with the highest score.
        float highest = Float.NEGATIVE_INFINITY;
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
     * Returns the most suitable parameterized preparators as the suggestions for the current data preparation step
     * in the pipeline according to the applicability scores of all the candidate preparators.
     *
     * @param pipeline the pipeline on which the decision engine process
     *
     * @return the most suitable parameterized preparator. Returning null indicates the termination of the process
     */
    public AbstractPreparator selectBestPreparators(AbstractPipeline pipeline) {
        // Todo: take locality into consideration
        // every time this method is called, instantiate all the preparator candidates again.
        initDecisionEngine();

        Dataset<Row> dataset = pipeline.getDataset();

        if (!hasColumn(dataset)) {
            // return null if the dataset is empty.
            return null;
        }

        // first the column combinations need to be generated.
        List<String> fieldName = Arrays.asList(dataset.schema().fieldNames());
        // using this permutation iterator cannot specify the maximal number of columns.
        SubsetIterator<String> iterator = new SubsetIterator<>(fieldName, MAX_COLUMN_COMBINATION_SIZE);

        while (iterator.hasNext()) {
            List<String> colNameCombination = iterator.next();

            // do not consider the column combination with no columns.
            if (colNameCombination.size()==0) {
                continue;
            }

            // create a new DataFrame including the columns specified by the column combination
            // Todo: for file-level and table-level preparators, e.g., remove preamble, there is no need to calculate the score for each column combination
            List<Column> columns = colNameCombination.stream().map(colName -> new Column(colName)).collect(Collectors.toList());
            Column[] columnArr = new Column[columns.size()];
            columnArr = columns.toArray(columnArr);
            Dataset<Row> dataSlice = dataset.select(columnArr);

            for (AbstractPreparator preparator : preparators) {
                float score = preparator.calApplicability(null, dataSlice, null);
                // the same preparator: only with the same class name
                float currentScore = scores.getOrDefault(preparator, Float.MIN_VALUE);
                if (currentScore==Float.MIN_VALUE) {
                    scores.put(preparator, score);
                }
                else {
                    if (score > currentScore) {
                        // replace the preparator instance in the key with the new preparator instance, though their names are the same.
                        scores.remove(preparator);
                        scores.put(preparator, score);
                    }
                }
            }

            // create new empty preparator instances that will be filled by parameters.
            instantiatePreparator();
        }

        // Find the preparator with the highest score.
        float highest = Float.NEGATIVE_INFINITY;
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Termination condition                                                                          //
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean stopProcess(AbstractPipeline pipeline) {
        if (pipeline.getSchemaMapping().hasMapped() && targetMetadataMapped(pipeline)) { // also the metadata are met
            return true;
        }
        return forceStop();
    }

    /**
     * When one of the termination conditions is met, return true to tell the decision engine stop the automation process.
     *
     * @return true if one of the termination conditions is met, otherwise false.
     */
    private boolean forceStop() {
        return iteration_count == MAX_ITERATION;
    }

    /**
     * Check whether the given dataset has any columns.
     *
     * @param dataset is the dataset to be checked
     * @return {@code false} if the dataset has no column.
     */
    private boolean hasColumn(Dataset<Row> dataset) {
        return dataset.columns().length > 0;
    }

    /**
     * Check whether all the target metadata have been fulfilled.
     *
     * @return
     */
    private boolean targetMetadataMapped(AbstractPipeline pipeline) {
        Set<Metadata> targetMetadata = pipeline.getTargetMetadata();
        MetadataRepository metadataRepository = pipeline.getMetadataRepository();

        // count the metadata in the target that have been fulfilled.
        int fulfilledCount = (int) targetMetadata.stream().filter(metadata -> {
            Metadata stored = metadataRepository.getMetadata(metadata);
            if (stored != null) {
                return stored.equalsByValue(metadata);
            } else {
                return false;
            }
        }).count();
        return fulfilledCount == targetMetadata.size();
    }

    public void setPreparatorCandidates(String[] preparatorCandidates) {
        DecisionEngine.preparatorCandidates = preparatorCandidates;
    }
}
