package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    
    // Instance variables to store field types and names
    private Type[] fieldTypes;
    private String[] fieldNames;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int totalFields = td1.numFields() + td2.numFields();
        Type[] combinedTypes = new Type[totalFields];
        String[] combinedNames = new String[totalFields];
        
        // Copy fields from td1
        for (int i = 0; i < td1.numFields(); i++) {
            combinedTypes[i] = td1.getType(i);
            combinedNames[i] = td1.getFieldName(i);
        }
        
        // Copy fields from td2
        for (int i = 0; i < td2.numFields(); i++) {
            combinedTypes[td1.numFields() + i] = td2.getType(i);
            combinedNames[td1.numFields() + i] = td2.getFieldName(i);
        }
        
        return new TupleDesc(combinedTypes, combinedNames);
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.fieldTypes = new Type[typeAr.length];
        this.fieldNames = new String[typeAr.length];
        
        // Copy the types
        for (int i = 0; i < typeAr.length; i++) {
            this.fieldTypes[i] = typeAr[i];
        }
        
        // Copy the field names (handle null fieldAr array)
        if (fieldAr != null) {
            for (int i = 0; i < typeAr.length; i++) {
                if (i < fieldAr.length) {
                    this.fieldNames[i] = fieldAr[i];
                } else {
                    this.fieldNames[i] = null;
                }
            }
        } else {
            // If fieldAr is null, all field names are null
            for (int i = 0; i < typeAr.length; i++) {
                this.fieldNames[i] = null;
            }
        }
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldTypes.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= fieldTypes.length) {
            throw new NoSuchElementException("Invalid field index: " + i);
        }
        return fieldNames[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("Field name cannot be null");
        }
        
        for (int i = 0; i < fieldNames.length; i++) {
            if (name.equals(fieldNames[i])) {
                return i;
            }
        }
        
        throw new NoSuchElementException("No field with name '" + name + "' found");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= fieldTypes.length) {
            throw new NoSuchElementException("Invalid field index: " + i);
        }
        return fieldTypes[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (Type type : fieldTypes) {
            totalSize += type.getLen();
        }
        return totalSize;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        TupleDesc other = (TupleDesc) o;
        
        // Check if they have the same number of fields
        if (this.numFields() != other.numFields()) {
            return false;
        }
        
        // Compare each field type
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getType(i).equals(other.getType(i))) {
                return false;
            }
        }
        
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldTypes.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldTypes[i].toString());
            sb.append("(");
            sb.append(fieldNames[i] != null ? fieldNames[i] : "null");
            sb.append(")");
        }
        return sb.toString();
    }
}