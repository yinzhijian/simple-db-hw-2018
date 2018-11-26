package simpledb;

import java.util.*;

import static simpledb.Aggregator.Op.AVG;
import static simpledb.Aggregator.Op.COUNT;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private List<Tuple> tuples;
    private Map<Field,Integer> f2cnt;
    private int cnt = 0;
    private TupleDesc desc;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        this.what = what;
        tuples = new ArrayList<>();
        f2cnt = new HashMap<>();
        if (gbfield == NO_GROUPING) {
            desc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            desc = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        boolean found = false;
        if (gbFieldType != null && gbField != NO_GROUPING) {
            Field gb = tup.getField(gbField);
            if (!f2cnt.containsKey(gb)) {
                f2cnt.put(gb,0);
            }
            f2cnt.put(gb,f2cnt.get(gb)+1);
            for (int i = 0;i < tuples.size();i++){
                if (tuples.get(i).getField(gbField).equals(gb)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                tuples.add(tup);
            }
        } else {
            cnt++;
            if (tuples.size() == 0) {
                tuples.add(tup);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private Boolean is_open = false;
            private int pos = 0;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (is_open) {
                    throw new IllegalStateException("Operator already open");
                }
                is_open = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!is_open) {
                    throw new IllegalStateException("Operator not yet open");
                }
                if (pos < tuples.size()) {
                    return true;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!is_open) {
                    throw new IllegalStateException("Operator not yet open");
                }
                if (pos < tuples.size()) {
                    Tuple tuple = tuples.get(pos);
                    Tuple newTuple = new Tuple(getTupleDesc());
                    if (what == COUNT) {
                        if (gbFieldType != null && gbField != NO_GROUPING) {
                            newTuple.setField(0,tuple.getField(gbField));
                            newTuple.setField(1, new IntField(f2cnt.get(newTuple.getField(0))));
                        } else {
                            newTuple.setField(0, new IntField(cnt));
                        }
                    }
                    pos++;
                    return newTuple;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!is_open) {
                    throw new IllegalStateException("Operator not yet open");
                }
                pos = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return desc;
            }

            @Override
            public void close() {
                is_open = false;
            }
        };
    }

}
