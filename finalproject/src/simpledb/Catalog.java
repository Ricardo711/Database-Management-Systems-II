package simpledb;

import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
    
    // Inner class to store table information
    private class TableInfo {
        public DbFile file;
        public TupleDesc tupleDesc;
        public String name;
        
        public TableInfo(DbFile file, TupleDesc tupleDesc, String name) {
            this.file = file;
            this.tupleDesc = tupleDesc;
            this.name = name;
        }
    }
    
    // Instance variables to store table information
    private Map<Integer, TableInfo> tableMap;  // Map from table ID to table info
    private Map<String, Integer> nameToIdMap;  // Map from table name to table ID

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this.tableMap = new HashMap<>();
        this.nameToIdMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.id() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, TupleDesc t, String name) {
        // some code goes here
        if (name == null) {
            throw new IllegalArgumentException("Table name cannot be null");
        }
        
        int tableId = file.id();
        TableInfo tableInfo = new TableInfo(file, t, name);
        
        // Add to table map (this will overwrite if table ID already exists)
        tableMap.put(tableId, tableInfo);
        
        // Handle name mapping - store all names including empty strings
        nameToIdMap.put(name, tableId);
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.id() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     */
    public void addTable(DbFile file, TupleDesc t) {
        addTable(file, t, "");
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("Table name cannot be null");
        }
        
        Integer tableId = nameToIdMap.get(name);
        if (tableId == null) {
            throw new NoSuchElementException("No table with name '" + name + "' found");
        }
        
        return tableId;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.id()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        TableInfo tableInfo = tableMap.get(tableid);
        if (tableInfo == null) {
            throw new NoSuchElementException("No table with id " + tableid + " found");
        }
        
        return tableInfo.tupleDesc;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.id()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        TableInfo tableInfo = tableMap.get(tableid);
        if (tableInfo == null) {
            throw new NoSuchElementException("No table with id " + tableid + " found");
        }
        
        return tableInfo.file;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        tableMap.clear();
        nameToIdMap.clear();
    }
}
