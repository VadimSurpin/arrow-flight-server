package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FlightScanBuilderTest {

    private static final String COLUMN_QUOTE = "\"";

    private static Configuration config() {
        return new Configuration("localhost", 32010, "user", "pass", null);
    }

    private static Table tableWithSchema() {
        Table t = Table.forTable("test_table", COLUMN_QUOTE);
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("score", DataTypes.FloatType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("active", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("l_quantity", DataTypes.createDecimalType(15, 2),
                        true, Metadata.empty()),
                new StructField("l_extendedprice", DataTypes.createDecimalType(15, 2),
                        true, Metadata.empty()),
                new StructField("l_discount", DataTypes.createDecimalType(15, 2),
                        true, Metadata.empty()),
                new StructField("l_tax", DataTypes.createDecimalType(15, 2),
                        true, Metadata.empty()),
                new StructField("l_returnflag", DataTypes.StringType, true, Metadata.empty()),
                new StructField("l_linestatus", DataTypes.StringType, true, Metadata.empty())
        });
        t.setSparkSchema(schema);
        return t;
    }

    private static PartitionBehavior noPartitioning() {
        return new PartitionBehavior(null, null, 1, null, null, null);
    }

    // ── pushFilters ───────────────────────────────────────────────────────

    /** Verifies Spark selects V2 filtering so column comparisons can reach Flight. */
    @Test
    void exposesOnlyV2FilterPushdownToSpark() {
        assertFalse(SupportsPushDownFilters.class.isAssignableFrom(FlightScanBuilder.class));
        assertTrue(SupportsPushDownV2Filters.class.isAssignableFrom(FlightScanBuilder.class));
    }

    @Test
    void pushFiltersReturnsUnhandledForUnsupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unsupported = new Filter[]{
                new EqualTo("id", 1),
                new AlwaysTrue()
        };
        Filter[] unhandled = builder.pushFilters(unsupported);
        assertEquals(1, unhandled.length);
        assertInstanceOf(AlwaysTrue.class, unhandled[0]);
        assertEquals(1, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersAllSupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] supported = new Filter[]{
                new EqualTo("id", 1),
                new LessThan("score", 100.0f)
        };
        Filter[] unhandled = builder.pushFilters(supported);
        assertEquals(0, unhandled.length);
        assertEquals(2, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersAllUnsupported() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unsupported = new Filter[]{
                new AlwaysTrue(),
                new AlwaysTrue()
        };
        Filter[] unhandled = builder.pushFilters(unsupported);
        assertEquals(2, unhandled.length);
        assertEquals(0, builder.pushedFilters().length);
    }

    @Test
    void pushFiltersEmptyArray() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        Filter[] unhandled = builder.pushFilters(new Filter[0]);
        assertEquals(0, unhandled.length);
        assertEquals(0, builder.pushedFilters().length);
    }

    @Test
    void pushPredicatesAcceptsColumnToColumnComparison() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        Predicate comparison = new Predicate("<", new Expression[]{
                FieldReference.column("l_commitdate"),
                FieldReference.column("l_receiptdate")
        });

        Predicate[] unhandled = builder.pushPredicates(new Predicate[]{comparison});

        assertEquals(0, unhandled.length);
        assertArrayEquals(new Predicate[]{comparison}, builder.pushedPredicates());
    }

    @Test
    void pushPredicatesReturnsUnsupportedExpressionsToSpark() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        Predicate unsupported = new Predicate(">", new Expression[]{
                new org.apache.spark.sql.connector.expressions.GeneralScalarExpression(
                        "+", new Expression[]{
                                FieldReference.column("id"),
                                new LiteralValue<>(1, DataTypes.IntegerType)
                        }),
                new LiteralValue<>(10, DataTypes.IntegerType)
        });

        Predicate[] unhandled = builder.pushPredicates(new Predicate[]{unsupported});

        assertArrayEquals(new Predicate[]{unsupported}, unhandled);
        assertEquals(0, builder.pushedPredicates().length);
    }

    // ── pruneColumns ──────────────────────────────────────────────────────

    @Test
    void pruneColumnsSetsRequiredColumns() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        StructType columns = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty())
        });
        builder.pruneColumns(columns);
    }

    // ── safe via pushAggregation with direct Spark API ────────────────────

    @Test
    void pushAggregationCountStar() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.CountStar()
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationCountStarWithGroupBy() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.CountStar()
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[]{
                                new org.apache.spark.sql.connector.expressions.NamedReference() {
                                    @Override
                                    public String[] fieldNames() {
                                        return new String[]{"active"};
                                    }

                                    @Override
                                    public String describe() {
                                        return "active";
                                    }
                                }
                        });
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationCountColumn() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Count(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationDistinctCountRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Count(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        true)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationMin() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Min(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        })
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationMax() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Max(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        })
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationSumDoubleAccepted() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationSumDecimalAccepted() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        reference("l_quantity"), false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);

        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationTpchQ1DecimalExpressionsAccepted() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        org.apache.spark.sql.connector.expressions.Expression one =
                new org.apache.spark.sql.connector.expressions.Cast(
                        new org.apache.spark.sql.connector.expressions.LiteralValue<>(
                                1, DataTypes.IntegerType),
                        DataTypes.createDecimalType(1, 0));
        org.apache.spark.sql.connector.expressions.Expression discountFactor =
                new org.apache.spark.sql.connector.expressions.GeneralScalarExpression(
                        "-", new org.apache.spark.sql.connector.expressions.Expression[]{
                                one, reference("l_discount")
                        });
        org.apache.spark.sql.connector.expressions.Expression discountedPrice =
                new org.apache.spark.sql.connector.expressions.GeneralScalarExpression(
                        "*", new org.apache.spark.sql.connector.expressions.Expression[]{
                                reference("l_extendedprice"), discountFactor
                        });
        org.apache.spark.sql.connector.expressions.Expression taxFactor =
                new org.apache.spark.sql.connector.expressions.GeneralScalarExpression(
                        "+", new org.apache.spark.sql.connector.expressions.Expression[]{
                                one, reference("l_tax")
                        });
        org.apache.spark.sql.connector.expressions.Expression charge =
                new org.apache.spark.sql.connector.expressions.GeneralScalarExpression(
                        "*", new org.apache.spark.sql.connector.expressions.Expression[]{
                                discountedPrice, taxFactor
                        });

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        reference("l_quantity"), false),
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        reference("l_extendedprice"), false),
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        discountedPrice, false),
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        charge, false),
                                new org.apache.spark.sql.connector.expressions.aggregate.CountStar()
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[]{
                                reference("l_returnflag"), reference("l_linestatus")
                        });

        assertTrue(builder.pushAggregation(agg));
    }

    @Test
    void pushAggregationSumIntegerRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"id"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "id";
                                            }
                                        },
                                        false)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "SUM on Integer column must be rejected");
    }

    @Test
    void pushAggregationSumDistinctRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                        new org.apache.spark.sql.connector.expressions.NamedReference() {
                                            @Override
                                            public String[] fieldNames() {
                                                return new String[]{"amount"};
                                            }
                                            @Override
                                            public String describe() {
                                                return "amount";
                                            }
                                        },
                                        true)
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "DISTINCT SUM must be rejected");
    }

    @Test
    void pushAggregationWithCustomAggregateRejected() {
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                                new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc() {
                                    @Override
                                    public String toString() {
                                        return "custom_agg";
                                    }

                                    @Override
                                    public org.apache.spark.sql.connector.expressions.Expression[]
                                            children() {
                                        return new org.apache.spark.sql.connector.expressions
                                                .Expression[0];
                                    }
                                }
                        },
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg),
                "Unknown aggregate function must be rejected");
    }

    /**
     * Creates a connector named reference for aggregation tests.
     *
     * @param column column name
     * @return named reference
     */
    private static org.apache.spark.sql.connector.expressions.NamedReference reference(
            String column) {
        return new org.apache.spark.sql.connector.expressions.NamedReference() {
            @Override
            public String[] fieldNames() {
                return new String[]{column};
            }

            @Override
            public String describe() {
                return column;
            }
        };
    }
}
