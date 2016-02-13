package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;

    private HashMap<Field, ArrayList<StringField>> tupleStorage;
    private ArrayList<StringField> tupleStorageNoGrouping;
    
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
            throw new IllegalArgumentException("Only COUNT aggregator for String is supported.");
        } else {
            this.gbField = gbfield;
            this.gbFieldType = gbfieldtype;
            this.aField = afield;
            this.op = what;
            this.op = what;

            if (isGrouping()) {
                this.tupleStorage = new HashMap<Field, ArrayList<StringField>>();
            } else {
                this.tupleStorageNoGrouping = new ArrayList<StringField>();
            }
        }
    }
    
    public boolean isGrouping() {
        return this.gbField != NO_GROUPING && this.gbFieldType != null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (isGrouping()) {
            Field gbValue = tup.getField(this.gbField);
            if (this.tupleStorage.containsKey(gbValue)) {
                this.tupleStorage.get(gbValue).add((StringField)tup.getField(this.aField));
            } else {
                this.tupleStorage.put(gbValue, new ArrayList<StringField>());
                this.tupleStorage.get(gbValue).add((StringField)tup.getField(this.aField));
            }
        } else {
            this.tupleStorageNoGrouping.add((StringField)tup.getField(this.aField));
        }
    }
    
    public IntField getCount(ArrayList<StringField> fields) {
        return new IntField(fields.size());
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
        /* iterator() call recalculates all the aggregates, which may not be the efficient solution */
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        if (isGrouping()) {
            Type[] typeAr = new Type[2];
            typeAr[0] = this.gbFieldType;
            typeAr[1] = Type.INT_TYPE;
            td = new TupleDesc(typeAr);

            for (Map.Entry<Field, ArrayList<StringField>> entry : this.tupleStorage.entrySet()) {
                IntField aggregate = this.getCount(entry.getValue());
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
            tup.setField(0, this.getCount(this.tupleStorageNoGrouping));
            tuples.add(tup);
        }

        TupleIterator iterator = new TupleIterator(td, tuples);
        return iterator;
    }

}
