package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import de.hpi.isg.dataprep.preparators.define.StemPreparator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by danthe on 26.11.18.
 */
public class StemTest extends PreparatorTest {

    @Test
    public void testValidColumn() throws Exception {
        Preparator preparator = new StemPreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testInValidColumn() throws Exception {
        Preparator preparator = new StemPreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
//        String[] rows = new String[]{""};
//        for(Iterator<Row> iter = pipeline.getRawData().toLocalIterator(), int i=0; iter.hasNext();i++){
//            Row row = iter.next();
//        }
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

}
