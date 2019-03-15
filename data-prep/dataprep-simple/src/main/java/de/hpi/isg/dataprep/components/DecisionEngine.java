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
 * The decision engine makes decisions such as selecting most suitable preparator for recommendation during the preparation
 * execution period.
 *
 * @author lan.jiang
 * @since 12/17/18
 */
public class DecisionEngine implements Engine {

    private final static int MAX_ITERATION = 10;
    private int iteration_count = 0;

    private final static String PREPARATOR_PACKAGE_PATH = "de.hpi.isg.dataprep.preparators.define";

    private static DecisionEngine instance;

    /**
     * specifies the preparator candidates that the decision engine may call. Could be moved to the controller.
     */
    private static String[] preparatorCandidates = {"AddProperty", "Collapse", "DeleteProperty", "Hash","MergeAttribute"};

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
        instantiatePreparator();
    }

    /**
     * Instantiate the preparator classes by their default constructor.
     */
    private void instantiatePreparator() {
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

    private void checkPipelineConfiguaration(AbstractPipeline pipeline) {
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
        checkPipelineConfiguaration(pipeline);
        SchemaMapping schemaMapping = pipeline.getSchemaMapping();
        Set<Metadata> targetMetadata = pipeline.getTargetMetadata();

        // every time this method is called, instantiate all the preparator candidates again.
        initDecisionEngine();

        Dataset<Row> dataset = pipeline.getDataset();
        // first the column combinations need to be generated.
        StructField[] fields = dataset.schema().fields();
        List<String> fieldName = Arrays.stream(fields).map(field -> field.name()).collect(Collectors.toList());

        if (fields.length == 0) {
            // all the fields are deleted.
            return null;
        }

        // using this permutation iterator cannot specify the maximal number of columns.
        SubsetIterator<String> iterator = new SubsetIterator<>(fieldName, 2);
        while (iterator.hasNext()) {
            List<String> colNameCombination = iterator.next();

            // do not consider the column combination with no columns.
            if (colNameCombination.size()==0) {
                continue;
            }

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

    public void printPreparatorCandidates() {
        for (String preparatorName : preparatorCandidates) {
            System.out.print(preparatorName + ", ");
        }
        System.out.println();
    }

    // Todo: the decision engine needs to notify the pipeline that the dataset needs to be updated, after executing a recommended preparator.
}
