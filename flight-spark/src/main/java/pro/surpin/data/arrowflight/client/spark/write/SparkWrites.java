package pro.surpin.data.arrowflight.client.spark.write;

import pro.surpin.data.arrowflight.client.write.WriteStatement;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Builds and fills literal-SQL write statements from Spark rows.
 */
public final class SparkWrites {

    /**
     * Prevents instantiation.
     */
    private SparkWrites() {
    }

    /**
     * Creates an insert statement, or a merge statement when merge columns are given.
     * @param table target table
     * @param mergeByColumns merge key columns, or null for insert
     * @param dataSchema Spark schema of the written rows
     * @param arrowSchema Arrow schema of the target table
     * @param columnQuote identifier quote
     * @param typeMapping Arrow-to-server type overrides
     * @return write statement
     */
    public static WriteStatement statement(String table, String[] mergeByColumns, StructType dataSchema,
                                           Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        return mergeByColumns == null
                ? new WriteStatement(table, dataSchema, arrowSchema, columnQuote, typeMapping)
                : new WriteStatement(table, mergeByColumns, dataSchema, arrowSchema, columnQuote, typeMapping);
    }

    /**
     * Renders the statement with the values of the given rows.
     * @param stmt write statement
     * @param dataSchema Spark schema of the rows
     * @param rows rows to write
     * @param arrowFields target Arrow fields
     * @return SQL with literal values
     */
    public static String fill(WriteStatement stmt, StructType dataSchema, InternalRow[] rows, Field[] arrowFields) {
        return stmt.fillStatement(rows, arrowFields);
    }
}
