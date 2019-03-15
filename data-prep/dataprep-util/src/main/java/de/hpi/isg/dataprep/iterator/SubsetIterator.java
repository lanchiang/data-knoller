package de.hpi.isg.dataprep.iterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator over all the subsets of a given set.
 *
 * @author lan.jiang
 * @since 1/20/19
 */
public class SubsetIterator<T> implements Iterator<List<T>> {

    private int maxNumElement;
    private List<T> elements;

    private int count;
    private int maxCount;
    private String padFormat;

    public SubsetIterator(final List<T> elements, int maxNumElement) {
        this.elements = elements;
        padFormat = "%"+elements.size()+"s";

        if (maxNumElement>elements.size()) {
            throw new RuntimeException(new IllegalArgumentException("Maximum number of elements exceeds the largest."));
        }

        this.maxNumElement = maxNumElement;
        this.maxCount = (int) Math.pow(2, elements.size());
    }

    public SubsetIterator(final List<T> elements) {
        this(elements, elements.size());
    }

    @Override
    public boolean hasNext() {
        return count != maxCount;//        if (Integer.bitCount(count)>elements.size()) {
//            return false;
//        }
    }

    @Override
    public List<T> next() {
        List<T> result = new ArrayList<>();
        while (hasNext()) {
            // if there are more elements than required in the collection, skip this iteration.
            if (Integer.bitCount(count)>maxNumElement) {
                count++;
                continue;
            }
            String binaryString = String.format(padFormat, Integer.toBinaryString(count)).replace(' ', '0');
            char[] binaryArray = binaryString.toCharArray();
            for (int i=0; i<elements.size(); i++) {
                if (binaryArray[i]=='1') {
                    result.add(elements.get(i));
                }
            }
            count++;
            break;
        }
        return result;
    }
}
