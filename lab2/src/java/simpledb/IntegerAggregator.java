package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;

    private HashMap<Field, ArrayList<IntField>> tupleStorage;
    private ArrayList<IntField> tupleStorageNoGrouping;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;

        if (isGrouping()) {
            this.tupleStorage = new HashMap<Field, ArrayList<IntField>>();
        } else {
            this.tupleStorageNoGrouping = new ArrayList<IntField>();
        }
    }

    public Boolean isGrouping() {
        return this.gbField != NO_GROUPING && this.gbFieldType != null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (isGrouping()) {
            Field gbValue = tup.getField(this.gbField);
            if (this.tupleStorage.containsKey(gbValue)) {
                this.tupleStorage.get(gbValue).add((IntField)tup.getField(this.aField));
            } else {
                this.tupleStorage.put(gbValue, new ArrayList<IntField>());
                this.tupleStorage.get(gbValue).add((IntField)tup.getField(this.aField));
            }
        } else {
            this.tupleStorageNoGrouping.add((IntField)tup.getField(this.aField));
        }
    }

    /* This should be called with fields.size() > 0; similar for the following functions */
    public IntField getAvg(ArrayList<IntField> fields) {
        int sum = 0;
        for (int i = 0; i < fields.size(); i++) {
            sum += fields.get(i).getValue();
        }
        return new IntField(sum / fields.size());
    }

    public IntField getMin(ArrayList<IntField> fields) {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getValue() < min) {
                min = fields.get(i).getValue();
            }
        }
        return new IntField(min);
    }

    public IntField getMax(ArrayList<IntField> fields) {
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getValue() > max) {
                max = fields.get(i).getValue();
            }
        }
        return new IntField(max);
    }

    public IntField getCount(ArrayList<IntField> fields) {
        return new IntField(fields.size());
    }

    public IntField getSum(ArrayList<IntField> fields) {
        int sum = 0;
        for (int i = 0; i < fields.size(); i++) {
            sum += fields.get(i).getValue();
        }
        return new IntField(sum);
    }

    public IntField getAggregate(ArrayList<IntField> fields) {
        switch (this.op) {
            case MIN:
                if (fields.size() > 0) {
                    return this.getMin(fields);
                } else {
                    return null;
                }
            case MAX:
                if (fields.size() > 0) {
                    return this.getMax(fields);
                } else {
                    return null;
                }
            case AVG:
                if (fields.size() > 0) {
                    return this.getAvg(fields);
                } else {
                    return null;
                }
            case SUM:
                return this.getSum(fields);
            case COUNT:
                return this.getCount(fields);
        }
        // we return null for aggregation op that we don't recognize
        return null;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        /* iterator() call recalculates all the aggregates, which may not be the efficient solution */
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        if (isGrouping()) {
            Type[] typeAr = new Type[2];
            typeAr[0] = this.gbFieldType;
            typeAr[1] = Type.INT_TYPE;
            td = new TupleDesc(typeAr);

            for (Map.Entry<Field, ArrayList<IntField>> entry : this.tupleStorage.entrySet()) {
                IntField aggregate = this.getAggregate(entry.getValue());
                Tuple tup = new Tuple(td);
                tup.setField(0, entry.getKey());
                tup.setField(1, aggregate);
                tuples.add(tup);
            }
        } else {
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            td = new TupleDesc(typeAr);

            Tuple tup = new Tuple(td);
            tup.setField(0, this.getAggregate(this.tupleStorageNoGrouping));
            tuples.add(tup);
        }

        TupleIterator iterator = new TupleIterator(td, tuples);
        return iterator;
    }
}
