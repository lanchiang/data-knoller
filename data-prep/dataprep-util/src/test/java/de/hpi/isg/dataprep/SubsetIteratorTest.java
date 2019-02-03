package de.hpi.isg.dataprep;

import de.hpi.isg.dataprep.iterator.SubsetIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class SubsetIteratorTest {

    private final static String[] preparatorCandidates = {"SplitProperty", "MergeProperty", "ChangeDateFormat", "RemovePreamble",
            "ChangePhoneFormat", "ChangeEncoding", "StemPreparator"};

    @Test
    public void instantiateTest() {
        List<String> preparatorCandidateList = Arrays.asList(preparatorCandidates);
        SubsetIterator<String> iterator = new SubsetIterator<>(preparatorCandidateList, 3);
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
    }
}
