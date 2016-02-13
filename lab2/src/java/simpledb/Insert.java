package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private Boolean called;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        if (child == null || Database.getCatalog().getTupleDesc(tableid) == null) {
            throw new DbException("TupleDesc empty.");
        }
        if (child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {
            this.tid = t;
            this.child = child;
            this.tableId = tableid;
            this.called = false;
        } else {
            throw new DbException("TupleDesc differs for insertion.");
        }
        
    }

    public TupleDesc getTupleDesc() {
        // This meant the tupleDesc of this operator, not that of its children
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        TupleDesc tupleDesc = new TupleDesc(typeAr);
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
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
        if (this.called) {
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        try {
            while (this.child.hasNext()) {
                bufferPool.insertTuple(this.tid, this.tableId, this.child.next());
                count += 1;
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        Tuple resultTuple = new Tuple(new TupleDesc(typeAr));
        resultTuple.setField(0, new IntField(count));
        this.called = true;
        return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] childrenArray = new DbIterator[1];
        childrenArray[0] = this.child;
        return childrenArray;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length > 0) {
            this.child = children[0];
        }
    }
}
