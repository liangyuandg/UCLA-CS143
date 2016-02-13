package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.child = child;
        this.tid = t;
        this.called = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Similar with insert, we return null if the deletion is called more than once
        if (this.called) {
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;
        try {
            while (this.child.hasNext()) {
                bufferPool.deleteTuple(this.tid, this.child.next());
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
