package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc desc;
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        desc = new TupleDesc(new Type[]{Type.INT_TYPE});
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        called = false;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called) {
            return null;
        }
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
                cnt++;
            } catch (IOException e){
                throw new DbException(e.toString());
            }
        }
        Tuple t = new Tuple(desc);
        t.setField(0,new IntField(cnt));
        called = true;
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        close();
        child = children[0];
    }
}
