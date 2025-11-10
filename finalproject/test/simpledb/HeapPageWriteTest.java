package simpledb;

import simpledb.TestUtil.SkeletonFile;

import java.io.IOException;
import java.util.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class HeapPageWriteTest {

    private HeapPageId pid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void setUp() throws IOException {
        this.pid = new HeapPageId(-1, -1);
        Database.getCatalog().addTable(new SkeletonFile(-1), Utility.getTupleDesc(2));
    }

    /**
     * Unit test for HeapPage.addTuple()
     */
    @Test public void addTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        int free = page.getNumEmptySlots();

        // NOTE(ghuo): this nested loop existence check is slow, but it
        // shouldn't make a difference for n = 512 slots.
        for (int i = -492; i <= -1; ++i) {
            Tuple addition = Utility.getHeapTuple(i, 2);
            page.addTuple(addition);
            assertEquals(free - (i + 493), page.getNumEmptySlots());

            // loop through the iterator to ensure that the tuple actually exists
            // on the page
            Iterator<Tuple >it = page.iterator();
            boolean found = false;
            while (it.hasNext()) {
                Tuple tup = it.next();
                if (TestUtil.compareTuples(addition, tup)) {
                    found = true;

                    // verify that the RecordID is sane
                    assertEquals(page.id(), tup.getRecordID().pageid());
                    break;
                }
            }
            assertTrue(found);
        }

        // now, the page should be full.
        try {
            page.addTuple(Utility.getHeapTuple(0, 2));
            throw new Exception("page should be full; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

    /**
     * Unit test for HeapPage.deleteTuple() with false tuples
     */
    @Test(expected=DbException.class)
        public void deleteNonexistentTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        page.deleteTuple(Utility.getHeapTuple(2, 2));
    }

    /**
     * Unit test for HeapPage.deleteTuple()
     */
    @Test public void deleteTuple() throws Exception {
        HeapPage page = new HeapPage(pid, HeapPageReadTest.EXAMPLE_DATA);
        int free = page.getNumEmptySlots();

        // first, build a list of the tuples on the page.
        Iterator<Tuple> it = page.iterator();
        LinkedList<Tuple> tuples = new LinkedList<Tuple>();
        while (it.hasNext())
            tuples.add(it.next());
        Tuple first = tuples.getFirst();

        // now, delete them one-by-one from both the front and the end.
        int deleted = 0;
        while (tuples.size() > 0) {
            page.deleteTuple(tuples.removeFirst());
            page.deleteTuple(tuples.removeLast());
            deleted += 2;
            assertEquals(free + deleted, page.getNumEmptySlots());
        }

        // now, the page should be empty.
        try {
            page.deleteTuple(first);
            throw new Exception("page should be empty; expected DbException");
        } catch (DbException e) {
            // explicitly ignored
        }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapPageWriteTest.class);
    }
}

