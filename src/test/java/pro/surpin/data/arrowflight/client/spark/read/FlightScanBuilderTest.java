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

    // ── pushdown selectivity estimation ───────────────────────────────────

    @Test
    void selectivityOfEmptyPredicatesIsOne() {
        assertEquals(1.0, FlightScanBuilder.estimateSelectivity(new Predicate[0]), 1e-9);
    }

    @Test
    void selectivityOfEqualityIsTenPercent() {
        Predicate eq = new EqualTo("id", 1).toV2();
        assertEquals(0.1, FlightScanBuilder.estimateSelectivity(new Predicate[]{eq}), 1e-9);
    }

    @Test
    void selectivityOfRangeIsOneThird() {
        Predicate lt = new LessThan("score", 100.0f).toV2();
        assertEquals(0.3333, FlightScanBuilder.estimateSelectivity(new Predicate[]{lt}), 1e-4);
    }

    @Test
    void selectivityOfConjunctionMultiplies() {
        // A conjunctive predicate array multiplies: equality (0.1) * range (1/3).
        Predicate[] both = new Predicate[]{
                new EqualTo("id", 1).toV2(),
                new GreaterThan("score", 10.0f).toV2()
        };
        assertEquals(0.1 * 0.3333, FlightScanBuilder.estimateSelectivity(both), 1e-4);
    }

    @Test
    void selectivityOfIsNotNullBarelyReduces() {
        Predicate notNull = new IsNotNull("id").toV2();
        assertEquals(0.9, FlightScanBuilder.estimateSelectivity(new Predicate[]{notNull}), 1e-9);
    }

    @Test
    void selectivityOfInScalesWithListSize() {
        Predicate in = new In("id", new Object[]{1, 2, 3}).toV2();
        // 3 alternatives * 0.1 = 0.3, capped at 0.5.
        assertEquals(0.3, FlightScanBuilder.estimateSelectivity(new Predicate[]{in}), 1e-9);
    }

    // ── aggregate pushdown cost gating ─────────────────────────────────────

    private static org.apache.spark.sql.connector.expressions.aggregate.Aggregation sumAmount() {
        return new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[]{
                        new org.apache.spark.sql.connector.expressions.aggregate.Sum(
                                FieldReference.apply("amount"), false)
                },
                new org.apache.spark.sql.connector.expressions.Expression[0]);
    }

    @Test
    void aggregatePushedWhenEstimatedInputExceedsThreshold() {
        Table t = tableWithSchema();
        Table.recordRawRowCount(t.getName(), 6_000_000L);
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        // Weak filter (one range ~1/3): 6M * 0.333 = 2M >= 500k default -> push.
        builder.pushPredicates(new Predicate[]{new LessThan("score", 100.0f).toV2()});
        assertTrue(builder.pushAggregation(sumAmount()));
    }

    @Test
    void aggregateDeclinedWhenSelectiveFilterShrinksInputBelowThreshold() {
        Table t = tableWithSchema();
        Table.recordRawRowCount(t.getName(), 6_000_000L);
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        // Three equalities: 6M * 0.1^3 = 6000 < 500k default -> decline, Spark aggregates.
        builder.pushPredicates(new Predicate[]{
                new EqualTo("id", 1).toV2(),
                new EqualTo("active", true).toV2(),
                new EqualTo("name", "x").toV2()
        });
        assertFalse(builder.pushAggregation(sumAmount()));
    }

    @Test
    void aggregatePushedWhenRawRowCountUnknown() {
        Table t = Table.forTable("unseen_table_for_gating", COLUMN_QUOTE);
        t.setSparkSchema(tableWithSchema().getSparkSchema());
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());
        builder.pushPredicates(new Predicate[]{new EqualTo("id", 1).toV2()});
        // No cached raw count -> cannot gate -> push (preserves prior behavior).
        assertTrue(builder.pushAggregation(sumAmount()));
    }

    @Test
    void aggregateGatingDisabledByNonPositiveThreshold() {
        Table t = tableWithSchema();
        Table.recordRawRowCount(t.getName(), 6_000_000L);
        Configuration cfg = config();
        cfg.setAggregatePushdownMinRows(0);
        FlightScanBuilder builder = new FlightScanBuilder(cfg, t, noPartitioning());
        builder.pushPredicates(new Predicate[]{
                new EqualTo("id", 1).toV2(),
                new EqualTo("active", true).toV2(),
                new EqualTo("name", "x").toV2()
        });
        assertTrue(builder.pushAggregation(sumAmount()), "threshold<=0 disables gating");
    }

    @Test
    void pushAggregationRejectsEmptyAggregation() {
        // Spark can offer an aggregation with no functions and no grouping keys on
        // a re-optimization pass for a global aggregate. Accepting it renders an
        // empty projection ("select  from ...") that fails server-side parsing
        // (TPC-H Q6), so the builder must decline.
        Table t = tableWithSchema();
        FlightScanBuilder builder = new FlightScanBuilder(config(), t, noPartitioning());

        org.apache.spark.sql.connector.expressions.aggregate.Aggregation agg =
                new org.apache.spark.sql.connector.expressions.aggregate.Aggregation(
                        new org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc[0],
                        new org.apache.spark.sql.connector.expressions.Expression[0]);
        assertFalse(builder.pushAggregation(agg));
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
