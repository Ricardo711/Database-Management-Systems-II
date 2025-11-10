package simpledb;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {
    
    // Instance variables to store tuple data
    private TupleDesc tupleDesc;
    private Field[] fields;
    private RecordID recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.fields = new Field[td.numFields()];
        this.recordId = null; // Initially null until set
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordID representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordID getRecordID() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordID information for this tuple.
     * @param rid the new RecordID for this tuple.
     */
    public void setRecordID(RecordID rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= fields.length) {
            throw new IndexOutOfBoundsException("Invalid field index: " + i);
        }
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.length) {
            throw new IndexOutOfBoundsException("Invalid field index: " + i);
        }
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append("\t");
            }
            if (fields[i] != null) {
                sb.append(fields[i].toString());
            } else {
                sb.append("null");
            }
        }
        sb.append("\n");
        return sb.toString();
    }
}
