package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Map<Field, Integer> groups = new HashMap<Field, Integer>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate for a group
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        Integer c = groups.get(key);
        if (c == null) c = 0;
        c += 1;
        groups.put(key, c);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        List<Tuple> out = new ArrayList<Tuple>();
        TupleDesc td;
        boolean grouping = (gbfield != NO_GROUPING);
        if (grouping) {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE },
                               new String[] { null, null });
            for (Map.Entry<Field, Integer> e : groups.entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, e.getKey());
                t.setField(1, new IntField(e.getValue()));
                out.add(t);
            }
        } else {
            td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { null });
            int v = 0;
            Integer c = groups.get(null);
            if (c != null) v = c;
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(v));
            out.add(t);
        }
        return new TupleIterator(td, out);
    }

}
