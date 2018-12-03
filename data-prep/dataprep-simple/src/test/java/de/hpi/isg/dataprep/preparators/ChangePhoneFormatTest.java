package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.metadata.DINPhoneNumber;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ChangePhoneFormat;
import org.junit.Assert;
import org.junit.Test;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.util.matching.Regex;

import java.util.ArrayList;
import java.util.List;

public class ChangePhoneFormatTest extends PreparatorTest {
    @Test
    public void changeFromSourceToTarget() throws Exception {
        ArrayList<String> areaGroups = new ArrayList<>(); areaGroups.add("areaCode"); areaGroups.add("number");
        Seq<String> areaGroupsSequence = JavaConversions.asScalaBuffer(areaGroups).toSeq();
        Regex areaCoded = new Regex("(\\d+) (\\d+)", areaGroupsSequence);
        DINPhoneNumber targetFormat = new DINPhoneNumber(false, true, false, false, areaCoded);

        ArrayList<String> specialGroups = new ArrayList<>(); specialGroups.add("areaCode"); specialGroups.add("specialNumber"); specialGroups.add("number");
        Seq<String> specialGroupsSequence = JavaConversions.asScalaBuffer(specialGroups).toSeq();
        Regex specialNumbered = new Regex("(\\d+) (\\d) (\\d+)", specialGroupsSequence);
        DINPhoneNumber sourceFormat = new DINPhoneNumber(false, true, true, false, specialNumbered);

        Preparator preparator = new ChangePhoneFormat("phoneNumber", sourceFormat, targetFormat);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }
}
