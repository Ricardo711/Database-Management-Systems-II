package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    // Instance variables to store scan parameters
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator dbFileIterator;

    /**
     * Constructor.
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.dbFileIterator = null;
    }

    /**
     * Opens this sequential scan.
     * Needs to be called before getNext().
     */
    public void open()
        throws DbException, TransactionAbortedException {
        // some code goes here
        // Get the DbFile for this table from the catalog
        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        
        // Get the iterator for this file
        dbFileIterator = dbFile.iterator(tid);
        
        // Open the iterator
        dbFileIterator.open();
    }

    /**
     * Implementation of DbIterator.getTupleDesc method.
     * Should return a tupleDesc with field names from the underlying HeapFile with field
     *   names prefaced by the passed in tableAlias string
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // Get the original TupleDesc from the catalog
        TupleDesc originalTd = Database.getCatalog().getTupleDesc(tableid);
        
        // Create arrays for the new field names and types
        Type[] types = new Type[originalTd.numFields()];
        String[] fieldNames = new String[originalTd.numFields()];
        
        // Copy types and create new field names with table alias prefix
        for (int i = 0; i < originalTd.numFields(); i++) {
            types[i] = originalTd.getType(i);
            String originalName = originalTd.getFieldName(i);
            
            // Create new field name as tableAlias.fieldName
            if (tableAlias != null && originalName != null) {
                fieldNames[i] = tableAlias + "." + originalName;
            } else if (tableAlias != null) {
                fieldNames[i] = tableAlias + ".null";
            } else if (originalName != null) {
                fieldNames[i] = "null." + originalName;
            } else {
                fieldNames[i] = "null.null";
            }
        }
        
        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (dbFileIterator == null) {
            return false;
        }
        return dbFileIterator.hasNext();
    }

    /**
     * Implementation of DbIterator.getNext method.
     * Return the next tuple in the scan, or null if there are no more tuples.
     *
     */
    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (dbFileIterator == null) {
            throw new NoSuchElementException("SeqScan not opened");
        }
        
        if (!hasNext()) {
            throw new NoSuchElementException("No more tuples");
        }
        
        return dbFileIterator.next();
    }

    /**
     * Closes the sequential scan.
     */
    public void close() {
        // some code goes here
        if (dbFileIterator != null) {
            dbFileIterator.close();
            dbFileIterator = null;
        }
    }

    /**
     * Rewinds the sequential back to the first record.
     */
    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        if (dbFileIterator != null) {
            dbFileIterator.rewind();
        }
    }
}
