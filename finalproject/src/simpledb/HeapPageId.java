package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    // Instance variables to store table ID and page number
    private int tableId;
    private int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int tableid() {
        // some code goes here
        return this.tableId;
    }

    /**
     * @return the page number in the table tableid() associated with
     *   this PageId
     */
    public int pageno() {
        // some code goes here
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        // Combine table ID and page number for hash code
        return Integer.hashCode(tableId) ^ Integer.hashCode(pgNo);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        HeapPageId other = (HeapPageId) o;
        return this.tableId == other.tableId && this.pgNo == other.pgNo;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        // Not necessary for this project
        return null;
    }

}
