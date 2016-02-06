package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItem
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return tdItems.listIterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tdItems = new ArrayList<TDItem>();
        // Check: when Type list length is 0, for now we just return, should write to log or throw an exception
        // throwing an exception would essentially change the function signature.
        if (typeAr != null && typeAr.length > 0) {
            // here we allow different lengths in typeAr and fieldAr
            for (int i = 0; i < typeAr.length; i++) {
                if (fieldAr != null && i < fieldAr.length && fieldAr[i] != null) {
                    tdItems.add(new TDItem(typeAr[i], fieldAr[i]));
                } else {
                    // we store null names in TDItem; this would cause problem in its toString() method;
                    // considered "", but getFieldName specified that its return could be null
                    tdItems.add(new TDItem(typeAr[i], null));

                    // we also don't care about name duplications
                }
            }
        }
        return;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
        return;
    }

    /**
     * Copy constructor. Create a copy of given TupleDesc.
     * @param TupleDesc
     *            TupleDesc to copy from
     */
    public TupleDesc(TupleDesc td1) {
        this.tdItems = new ArrayList<TDItem>();
        if (td1 == null) {
            return;
        }
        for (int i = 0; i < td1.numFields(); i++) {
            this.tdItems.add(new TDItem(td1.getFieldType(i), td1.getFieldName(i)));
        }
        return;
    }

    public void appendTDItem(TDItem tdItem) {
        this.tdItems.add(tdItem);
        return;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < this.tdItems.size()) {
            return this.tdItems.get(i).fieldName;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < this.tdItems.size()) {
            return this.tdItems.get(i).fieldType;
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        String fieldName;
        for (int i = 0; i < this.tdItems.size(); i++) {
            fieldName = this.getFieldName(i);
            if (fieldName != null && this.getFieldName(i).equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalLen = 0;
        for (int i = 0; i < this.tdItems.size(); i++) {
            totalLen += this.getFieldType(i).getLen();
        }
        return totalLen;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Use our copy constructor and append method
        TupleDesc result = new TupleDesc(td1);
        for (int i = 0; i < td2.numFields(); i++) {
            result.appendTDItem(new TDItem(td2.getFieldType(i), td2.getFieldName(i)));
        }
        return result;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        try {
            TupleDesc td1 = (TupleDesc) o;
            if (td1 != null && td1.numFields() == this.numFields()) {
                for (int i = 0; i < td1.numFields(); i++) {
                    if (!td1.getFieldType(i).equals(this.getFieldType(i))) {
                        return false;
                    }
                }
            } else {
                return false;
            }
            return true;
        } catch (ClassCastException e) {
            return false;
        }
    }

    public int hashCode() {
        // Even though we don't use TupleDesc as keys for HashMap, we still implement this in case of future need
        Integer hash = 0;
        for (int i = 0; i < this.numFields(); i++) {
            hash = Objects.hash(hash, this.getFieldType(i));
        }
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        if (this.tdItems.size() > 0) {
            // Since the tdItems should contain at least one entry per spec. 
            String result = this.tdItems.get(0).toString();
            for (int i = 1; i < tdItems.size(); i++) {
                // Q: sure you want "fieldType[0](fieldName[0])" instead of "fieldName[0](fieldType[0])"?
                // Not implemented as spec for now
                result += ", " + this.tdItems.get(i).toString();
            }
            return result;
        } else {
            return "";
        }
    }

    private List<TDItem> tdItems;
}
