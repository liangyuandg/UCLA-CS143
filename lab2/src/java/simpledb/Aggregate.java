package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op op;

    private Aggregator aggregator;

    private DbIterator aggregatedIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        this.op = aop;

        // we don't calculate aggregation immediately, because dbIterator child may not be open
        this.aggregatedIterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (this.gField == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return this.child.getTupleDesc().getFieldName(this.gField);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return this.op.toString() + " " + this.child.getTupleDesc().getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // we calculate aggregate upon open()   
        switch (this.child.getTupleDesc().getFieldType(this.aField)) {
            case INT_TYPE:
                if (this.gField == Aggregator.NO_GROUPING) {
                    this.aggregator = new IntegerAggregator(this.gField, null, this.aField, this.op);
                } else {
                    this.aggregator = new IntegerAggregator(this.gField, this.child.getTupleDesc().getFieldType(this.gField), this.aField, this.op);
                }
                break;
            case STRING_TYPE:
                if (this.gField == Aggregator.NO_GROUPING) {
                    this.aggregator = new StringAggregator(this.gField, null, this.aField, this.op);
                } else {
                    this.aggregator = new StringAggregator(this.gField, this.child.getTupleDesc().getFieldType(this.gField), this.aField, this.op);
                }
                break;
        }

        super.open();
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.aggregatedIterator = this.aggregator.iterator();
        if (this.aggregatedIterator != null) {
            this.aggregatedIterator.open();
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregatedIterator != null && this.aggregatedIterator.hasNext()) {
            return this.aggregatedIterator.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
        // we don't mess with the child iterator in this case, as if we rewind the child as well, 
        // we'll end up with duplicated entries in aggregator
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // for now, all of our aggregates result in INT_TYPE
        if (this.gField == Aggregator.NO_GROUPING) {
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            String[] nameAr = new String[1];
            nameAr[0] = this.aggregateFieldName();
            return new TupleDesc(typeAr, nameAr);
        } else {
            Type[] typeAr = new Type[2];
            typeAr[0] = this.child.getTupleDesc().getFieldType(this.aField);
            typeAr[1] = Type.INT_TYPE;
            String[] nameAr = new String[2];
            nameAr[0] = this.groupFieldName();
            nameAr[1] = this.aggregateFieldName();
            return new TupleDesc(typeAr, nameAr);
        }
    }

    public void close() {
        super.close();
        this.child.close();
        if (this.aggregatedIterator != null) {
            this.aggregatedIterator.close();
        }
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
