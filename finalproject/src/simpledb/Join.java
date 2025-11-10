package simpledb;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends AbstractDbIterator {

    private final JoinPredicate p;
    private final DbIterator child1;
    private final DbIterator child2;
    private Tuple currentLeft;
    private TupleDesc mergedTd;

    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.currentLeft = null;
        this.mergedTd = TupleDesc.combine(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        currentLeft = null;
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
        currentLeft = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        currentLeft = null;
    }

    /**
     * Returns the TupleDesc for this operator, combining the TupleDescs of its
     * two children.
     * 
     * @return the TupleDesc that combines the two TupleDescs of the children.
     */
    public TupleDesc getTupleDesc() {
        return mergedTd;
    }

    /**
     * Operator.fetchNext implementation.  Should return the next matching tuple,
     * or null if there are no more matching tuples.
     *
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right relation.
     * Therefore, if an equality predicate is used there will be two copies of the join
     * attribute in the results.  (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        while (true) {
            if (currentLeft == null) {
                if (child1.hasNext()) {
                    currentLeft = child1.next();
                } else {
                    return null;
                }
            }
            while (child2.hasNext()) {
                Tuple right = child2.next();
                if (p.filter(currentLeft, right)) {
                    Tuple out = new Tuple(mergedTd);
                    int idx = 0;
                    for (int i = 0; i < currentLeft.getTupleDesc().numFields(); i++) {
                        out.setField(idx++, currentLeft.getField(i));
                    }
                    for (int j = 0; j < right.getTupleDesc().numFields(); j++) {
                        out.setField(idx++, right.getField(j));
                    }
                    return out;
                }
            }
            // exhausted right, move to next left
            child2.rewind();
            currentLeft = null;
        }
    }
}
