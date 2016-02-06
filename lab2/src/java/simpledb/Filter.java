package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private DbIterator dbIterator;

    private DbIterator[] children;

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
        this.dbIterator = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.dbIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.dbIterator.open();
    }

    public void close() {
        super.close();
        this.dbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.dbIterator.rewind();
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
        while (this.dbIterator.hasNext()) {
            next = this.dbIterator.next();
            if (this.predicate.filter(next)) {
                return next;
            }
        }
        return null;
    }

    // TODO: The purpose of these methods?
    @Override
    public DbIterator[] getChildren() {
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.children = children;
    }

}
