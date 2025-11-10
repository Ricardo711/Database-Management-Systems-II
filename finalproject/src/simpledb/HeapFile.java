package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    // Instance variable to store the file
    private File file;
    
    /**
     * Constructor.
     * Creates a new heap file that stores pages in the specified buffer pool.
     *
     * @param f The file that stores the on-disk backing store for this DbFile.
     */
    public HeapFile(File f) {
        // some code goes here
        this.file = f;
    }

    /**
     * Return a Java File corresponding to the data from this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * @return an ID uniquely identifying this HeapFile
     *  (Implementation note:  you will need to generate this tableid somewhere,
     *    ensure that each HeapFile has a "unique id," and that you always
     *    return the same value for a particular HeapFile.  The implementation we
     *    suggest you use could hash the absolute file name of the file underlying
     *    the heapfile, i.e. f.getAbsoluteFile().hashCode()
     *    )
     */
    public int id() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns a Page from the file.
     */
    public Page readPage(PageId pid) throws NoSuchElementException {
        // some code goes here
        try {
            // Get the tuple descriptor to calculate the actual page layout
            TupleDesc td = Database.getCatalog().getTupleDesc(id());
            int tupleSize = td.getSize();
            int tuplesPerPage = BufferPool.PAGE_SIZE / tupleSize;
            int headerBytes = ((tuplesPerPage / 32) + 1) * 4;
            int bytesPerPageOnDisk = headerBytes + BufferPool.PAGE_SIZE;
            
            // Calculate the byte offset for this page
            int pageNumber = pid.pageno();
            long offset = (long) pageNumber * bytesPerPageOnDisk;
            
            // Check if the page number is valid
            if (pageNumber < 0 || pageNumber >= numPages()) {
                throw new NoSuchElementException("Page " + pageNumber + " does not exist in this file");
            }
            
            // Read the complete page data from file (header + page data)
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            
            byte[] pageData = new byte[bytesPerPageOnDisk];
            int bytesRead = raf.read(pageData);
            raf.close();
            
            // If we didn't read a full page, pad with zeros
            if (bytesRead < bytesPerPageOnDisk) {
                for (int i = bytesRead; i < bytesPerPageOnDisk; i++) {
                    pageData[i] = 0;
                }
            }
            
            // Create and return a HeapPage from the data
            return new HeapPage((HeapPageId) pid, pageData);
            
        } catch (IOException e) {
            throw new NoSuchElementException("Error reading page: " + e.getMessage());
        }
    }

    /**
     * Writes the given page to the appropriate location in the file.
     */
    public void writePage(Page page) throws IOException {
        // not necessary for this project
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Calculate the number of pages based on file size
        long fileSize = file.length();
        if (fileSize == 0) {
            return 0;
        }
        
        // Get the tuple descriptor to calculate bytes per page on disk
        TupleDesc td = Database.getCatalog().getTupleDesc(id());
        int tupleSize = td.getSize();
        int tuplesPerPage = BufferPool.PAGE_SIZE / tupleSize;
        int headerBytes = ((tuplesPerPage / 32) + 1) * 4;
        int bytesPerPageOnDisk = headerBytes + BufferPool.PAGE_SIZE;
        
        return (int) Math.ceil((double) fileSize / bytesPerPageOnDisk);
    }

    /**
     * Adds the specified tuple to the table under the specified TransactionId.
     *
     * @throws DbException
     * @throws IOException
     * @return An ArrayList contain the pages that were modified
     */
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	// not necessary for this project
        return null;
    }

    /**
     * Deletes the specified tuple from the table, under the specified
     * TransactionId.
     */
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	// not necessary for this project
        return null;
    }

    /**
     * An iterator over all tuples on this file, over all pages.
     * Note that this iterator should use BufferPool.getPage(), rather than HeapFile.getPage()
     * to iterate through pages.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageNo = 0;
            private Iterator<Tuple> currentPageIterator = null;
            private boolean isOpen = false;
            
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                currentPageNo = 0;
                currentPageIterator = null;
            }
            
            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    return false;
                }
                
                // If we have a current page iterator and it has more tuples, return true
                if (currentPageIterator != null && currentPageIterator.hasNext()) {
                    return true;
                }
                
                // Otherwise, try to find the next page with tuples
                while (currentPageNo < numPages()) {
                    try {
                        HeapPageId pageId = new HeapPageId(id(), currentPageNo);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                        currentPageIterator = page.iterator();
                        
                        if (currentPageIterator.hasNext()) {
                            return true;
                        }
                        
                        currentPageNo++;
                    } catch (Exception e) {
                        throw new DbException("Error accessing page " + currentPageNo + ": " + e.getMessage());
                    }
                }
                
                return false;
            }
            
            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more tuples");
                }
                
                Tuple tuple = currentPageIterator.next();
                
                // If this page is exhausted, move to the next page
                if (!currentPageIterator.hasNext()) {
                    currentPageNo++;
                    currentPageIterator = null;
                }
                
                return tuple;
            }
            
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
            
            @Override
            public void close() {
                isOpen = false;
                currentPageNo = 0;
                currentPageIterator = null;
            }
        };
    }

    /**
     * @return the number of bytes on a page, including the number of bytes
     * in the header.
     */
    public int bytesPerPage() {
        // some code goes here
        // Calculate total bytes per page on disk (header + page data)
        TupleDesc td = Database.getCatalog().getTupleDesc(id());
        int tupleSize = td.getSize();
        int tuplesPerPage = BufferPool.PAGE_SIZE / tupleSize;
        int headerBytes = ((tuplesPerPage / 32) + 1) * 4;
        return headerBytes + BufferPool.PAGE_SIZE;
    }
}

