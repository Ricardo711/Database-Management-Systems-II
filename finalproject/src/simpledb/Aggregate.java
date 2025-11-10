package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {

    private final DbIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;

    private Aggregator aggregator;
    private DbIterator aggIter;
    private TupleDesc td; // result schema

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or NO_GROUPING if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.aggregator = null;
        this.aggIter = null;
        this.td = null;
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        default:
            throw new IllegalStateException("impossible to reach here"); 
        }
    }

    public void open() throws NoSuchElementException, DbException,
        TransactionAbortedException {
        child.open();
        TupleDesc childTd = child.getTupleDesc();
        Type aType = childTd.getType(afield);
        Type gType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getType(gfield);

        if (aType == Type.INT_TYPE) {
            aggregator = new IntAggregator(gfield, gType, afield, aop);
        } else if (aType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gType, afield, aop);
        } else {
            throw new DbException("Unsupported aggregate field type");
        }

        // Merge all tuples
        while (child.hasNext()) {
            Tuple t = child.next();
            aggregator.merge(t);
        }

        // Build result iterator
        aggIter = aggregator.iterator();
        aggIter.open();

        // Build TupleDesc with names
        String aggFieldName = childTd.getFieldName(afield);
        String aggName = aggName(aop) + "(" + aggFieldName + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE },
                               new String[] { aggName });
        } else {
            String gname = childTd.getFieldName(gfield);
            td = new TupleDesc(new Type[] { childTd.getType(gfield), Type.INT_TYPE },
                               new String[] { gname, aggName });
        }
    }

    /**
     * Returns the next tuple. If there is no next tuple, return null.
     * Hint: query plans are not expected to handle huge amounts of data, so
     * merging all the data during open() is acceptable. 
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (aggIter != null && aggIter.hasNext()) {
            Tuple t = aggIter.next();
            // ensure returned tuple has our named TupleDesc
            Tuple out = new Tuple(td);
            for (int i = 0; i < td.numFields(); i++) {
                out.setField(i, t.getField(i));
            }
            return out;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if (aggIter != null) {
            aggIter.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no grouping, this will have one field - the aggregate result.
     * If there is grouping, this will have two fields, the group-by field and the aggregate result.
     *
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        return td == null ? computeTupleDesc(child.getTupleDesc()) : td;
    }

    private TupleDesc computeTupleDesc(TupleDesc childTd) {
        String aggFieldName = childTd.getFieldName(afield);
        String aggNameStr = aggName(aop) + "(" + aggFieldName + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[] { Type.INT_TYPE },
                                 new String[] { aggNameStr });
        } else {
            String gname = childTd.getFieldName(gfield);
            return new TupleDesc(new Type[] { childTd.getType(gfield), Type.INT_TYPE },
                                 new String[] { gname, aggNameStr });
        }
    }

    public void close() {
        super.close();
        if (aggIter != null) {
            aggIter.close();
            aggIter = null;
        }
        child.close();
    }
}
