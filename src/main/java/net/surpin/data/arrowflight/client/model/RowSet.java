package net.surpin.data.arrowflight.client.model;

import org.apache.arrow.vector.types.pojo.Schema;
import java.util.ArrayList;

/**
 * Describes row batch from remote flight service
 */
public class RowSet {
    /**
     * The definition of Row
     */
    public static class Row {
        private final ArrayList<Object> data;

        /**
         * Construct empty Row
         */
        public Row() {
            this.data = new ArrayList<>();
        }

        /**
         * Add value to Row
         * @param o value to add
         */
        public void add(Object o) {
            this.data.add(o);
        }

        /**
         * Get all values in Row
         * @return array of values
         */
        public Object[] getData() {
            return this.data.toArray(new Object[0]);
        }
    }

    //the schema of each ROw
    private final Schema schema;
    //the row collection
    private final ArrayList<Row> data;

    /**
     * Construct a RowSet
     * @param schema - the schema of each row in the collection
     */
    public RowSet(Schema schema) {
        this.schema = schema;
        this.data = new ArrayList<>();
    }

    /**
     * Get the schema of the RowSet
     * @return - the schema
     */
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * Add one Row
     * @param row - the row to be added
     */
    public void add(Row row) {
        this.data.add(row);
    }

    /**
     * Add all rows from another RowSet
     * @param rs - the input RowSet
     * @throws IllegalArgumentException if the schemas do not match
     */
    public void add(RowSet rs) {
        if (rs.schema != this.schema) {
            throw new IllegalArgumentException("The schema doesn't match. Cannot add the RowSet.");
        }
        this.data.addAll(rs.data);
    }

    /**
     * Get all Rows
     * @return - all rows in the RowSet
     */
    public Row[] getData() {
        return this.data.toArray(new Row[] {});
    }
}
