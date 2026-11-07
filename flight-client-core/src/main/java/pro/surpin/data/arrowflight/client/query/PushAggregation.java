package pro.surpin.data.arrowflight.client.query;

import java.io.Serializable;

/**
 * Describes the data structure for pushed-down aggregation
 */
public class PushAggregation implements Serializable {
    //pushed-down Aggregate-Columns (expressions)
    private String[] columnExpressions = null;
    //pushed-down GroupBy-Columns
    private String[] groupByColumns = null;
    //grouping column whose values the server must keep in one endpoint, so that results are final per endpoint
    private String completeBy = null;

    /**
     * Push down aggregation of columns
     *   select max(age), sum(distinct amount) from table where ...
     * @param columnExpressions - the collection of aggregation expressions
     */
    public PushAggregation(String[] columnExpressions) {
        this.columnExpressions = columnExpressions;
    }

    /**
     * Push down aggregation with group by columns
     *   select max(age), sum(amount) from table where ... group by gender
     * @param columnExpressions - the collection of aggregation expressions
     * @param groupByColumns - the columns in group by
     */
    public PushAggregation(String[] columnExpressions, String[] groupByColumns) {
        this(columnExpressions);
        this.groupByColumns = groupByColumns;
    }

    /**
     * Return the collection of aggregation expressions
     * @return - the expressions
     */
    public String[] getColumnExpressions() {
        return this.columnExpressions;
    }

    /**
     * The columns for group-by
     * @return - columns
     */
    public String[] getGroupByColumns() {
        return this.groupByColumns;
    }

    /**
     * The grouping column the server must keep whole: no value of it may be read by two endpoints, so each endpoint
     * returns final groups and the engine need not aggregate again.
     * @return - the column, or null when groups are only partial per endpoint
     */
    public String getCompleteBy() {
        return this.completeBy;
    }

    /**
     * Declares the aggregation final per endpoint, provided the server keeps the values of this column together.
     * @param column - one of the group-by columns, a plain identifier
     */
    public void setCompleteBy(String column) {
        this.completeBy = column;
    }
}
