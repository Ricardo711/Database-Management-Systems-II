package simpledb;

/**
 * A RecordID is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordID {
	
	private PageId pid;
	private int tupleno;

    /** Constructor.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordID(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return tupleno;
    }

    /**
     * @return the table id this RecordId references.
     */
    public PageId pageid() {
        return pid;
    }
}
