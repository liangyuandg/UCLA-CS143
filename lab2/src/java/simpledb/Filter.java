package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    /* TODO: write about the open() here doesn't mean opening the child iterator, same as Join.open()
             , Aggregate.open(), etc, but instead mean opening this iterator itself. 
             Child's open is separate; this may evade the test as the test always pass in opened children */
    /* Or instead, we implement it as opening/closing child anyway, as rewind will always mess with child iterator.
             This means that one dbIterator should be not be shared among different instances of Operator. */
    /* Conclusion: actually systemtest expects us to open/close child iterator here, so this implementation's fine. */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: double check this method
        Tuple next = null;
        while (this.child.hasNext()) {
            next = this.child.next();
            if (this.predicate.filter(next)) {
                return next;
            }
        }
        return null;
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
