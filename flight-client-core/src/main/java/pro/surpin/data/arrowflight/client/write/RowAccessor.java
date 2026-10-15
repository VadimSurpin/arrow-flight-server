package pro.surpin.data.arrowflight.client.write;

/**
 * Engine-neutral read access to one row that is being written.
 *
 * Values use plain Java types chosen by the column's FieldType: Number for integer and
 * floating-point columns, BigDecimal for DECIMAL, Boolean, String for VARCHAR and CHAR,
 * LocalDate for DATE, Instant for TIMESTAMP, List for LIST, Map for MAP, and an ordered
 * Map from field name to value for STRUCT. Null means SQL NULL.
 */
@FunctionalInterface
public interface RowAccessor {

    /**
     * Returns a column value.
     * @param column zero-based column index in the statement's data fields
     * @return value, or null
     */
    Object get(int column);
}
