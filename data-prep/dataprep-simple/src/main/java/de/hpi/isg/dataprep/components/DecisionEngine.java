package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.model.target.system.Engine;
import de.hpi.isg.dataprep.utility.ClassUtility;
import org.apache.commons.collections4.iterators.PermutationIterator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The decision engine makes decisions such as selecting most suitable preparator for recommendation during the preparation
 * execution period.
 *
 * @author lan.jiang
 * @since 12/17/18
 */
public class DecisionEngine implements Engine {

    private static DecisionEngine instance;

    private final static int MAX_ITERATION = 100;

    /**
     * specifies the preparator candidates that the decision engine may call. Could be moved to the controller.
     */
    private final static String[] preparatorCandidates = {"SplitProperty", "MergeProperty", "ChangeDateFormat", "RemovePreamble",
            "ChangePhoneFormat", "ChangeEncoding", "StemPreparator"};

    private DecisionEngine() {}

    // get the instance of the class only by this method
    public static DecisionEngine getInstance() {
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
    AbstractPreparator selectBestPreparator(Dataset<Row> dataset) {
        ClassUtility.checkClassesExistence(preparatorCandidates, "de.hpi.isg.dataprep.preparators.define");

        // proceed if all the classes exist

        // first the column combinations need to be generated.
        StructField[] fields = dataset.schema().fields();
        List<String> fieldName = Arrays.stream(fields).map(field -> field.name()).collect(Collectors.toList());

        // using this permutation iterator cannot specify the maximal number of columns.
        PermutationIterator<String> iterator = new PermutationIterator<>(fieldName);
        while (iterator.hasNext()) {
            List<String> colNameCombination = iterator.next();
//            List<Column> columns = colNameCombination.stream().map(colName -> new Column(colName)).collect(Collectors.toList());
//            Column[] columnArr = new Column[columns.size()];
//            columnArr = columns.toArray(columnArr);

            Column[] columnArr = colNameCombination.stream().toArray(Column[]::new);

            Dataset<Row> dataSlice = dataset.select(columnArr);
        }

        return null;
    }

    // Todo: the decision engine needs to notify the pipeline that the dataset needs to be updated, after executing a recommended preparator.
}
