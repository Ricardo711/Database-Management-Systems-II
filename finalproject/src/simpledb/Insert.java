package simpledb;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
// Not necessary for this project
public class Insert extends AbstractDbIterator {

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
    	 // Not necessary for this project
    }

    public TupleDesc getTupleDesc() {
    	 // Not necessary for this project
        return null;
    }

    public void open() throws DbException, TransactionAbortedException {
    	 // Not necessary for this project
    }

    public void close() {
    	 // Not necessary for this project
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	 // Not necessary for this project
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor.  Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert
     * DOES NOT need check to see if a particular tuple is a duplicate before
     * inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     * @throws NoSuchElementException When the child iterator has no more
     * tuples.
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    	 // Not necessary for this project
        return null;
    }
}
