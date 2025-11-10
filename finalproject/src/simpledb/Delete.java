package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
// Not necessary for this project
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be access via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
    	 // Not necessary for this project
        return null;
    }
}
