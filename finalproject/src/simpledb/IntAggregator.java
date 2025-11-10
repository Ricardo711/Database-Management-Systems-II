package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private static class AggState {
        int count = 0;
        int sum = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        void add(int v) {
            count++;
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        int value(Op op) {
            switch (op) {
                case COUNT: return count;
                case SUM: return sum;
                case AVG: return count == 0 ? 0 : (sum / count);
                case MIN: return count == 0 ? 0 : min;
                case MAX: return count == 0 ? 0 : max;
                default: throw new IllegalArgumentException("Unsupported op for IntAggregator");
            }
        }
    }

    private final Map<Field, AggState> groups = new HashMap<Field, AggState>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate for a group.
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField aval = (IntField) tup.getField(afield);
        AggState st = groups.get(key);
        if (st == null) {
            st = new AggState();
            groups.put(key, st);
        }
        st.add(aval.getValue());
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
            for (Map.Entry<Field, AggState> e : groups.entrySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, e.getKey());
                t.setField(1, new IntField(e.getValue().value(what)));
                out.add(t);
            }
        } else {
            td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { null });
            AggState st = groups.get(null);
            int v = (st == null) ? 0 : st.value(what);
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(v));
            out.add(t);
        }
        return new TupleIterator(td, out);
    }

}
