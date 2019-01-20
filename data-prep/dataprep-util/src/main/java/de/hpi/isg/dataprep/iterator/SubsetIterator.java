import java.util.Collection;
import java.util.Iterator;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class SubsetIterator<T> implements Iterator<Collection<T>> {

    private int maxNumElement;

    public SubsetIterator(int maxNumElement) {
        this.maxNumElement = maxNumElement;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Collection<T> next() {
        return null;
    }
}
