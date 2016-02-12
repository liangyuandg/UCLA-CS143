package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td.numFields() > 0) {
            this.tupleDesc = td;
            // this would actually cause problems when setting field, since it confuses capacity with size of a list
            //this.fields = new ArrayList<Field>(this.tupleDesc.numFields());
            this.fields = Arrays.asList(new Field[td.numFields()]);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
        return;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < this.tupleDesc.numFields()) {
            // to be exact (i + 1)th field of this tuple
            this.fields.set(i, f);
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (i < this.tupleDesc.numFields()) {
            return this.fields.get(i);
        } else {
            return null;
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // tupleDesc can't be empty, so tuple shouldn't be, either.
        if (this.fields.size() > 0) {
            String result = "";
            // Check: in this toString, if the field is not specified, we will leave its space blank
            if (this.getField(0) != null) {
                result = this.getField(0).toString();
            }
            for (int i = 1; i < this.fields.size(); i++) {
                result += "\t";
                if (this.getField(i) != null) {
                    result += this.getField(i).toString();
                }
            }
            result += "\n";
            return result;
        } else {
            return "";
        }
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return this.fields.listIterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleDesc = td;
    }

    private RecordId recordId;
    private TupleDesc tupleDesc;
    private List<Field> fields;
}
