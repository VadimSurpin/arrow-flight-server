## Flight/Direct (default configuration), median over schemas, per query

| query | Flight (s) | Direct (s) | median ratio | schemas Flight faster |
|---|---|---|---|---|
| count_distinct_sel1 | 0.72 | 1.06 | 0.70 | 22 of 22 |
| count_distinct_sel50 | 1.13 | 1.16 | 0.84 | 19 of 22 |
| filter_groupby_sel1 | 0.77 | 0.97 | 0.81 | 22 of 22 |
| filter_groupby_sel50 | 0.69 | 1.06 | 0.66 | 22 of 22 |
| groupby_key | 0.84 | 1.47 | 0.60 | 20 of 22 |
| join_agg_same_key | 1.45 | 1.72 | 0.71 | 21 of 22 |
| join_selective_dim | 1.34 | 1.06 | 1.25 | 4 of 22 |
| topn | 1.13 | 1.11 | 1.01 | 9 of 22 |

## Effect of each axis on Flight/Direct (total Flight time / total Direct time over the 8 queries)

| axis | value | schemas | Flight total (s) | Direct total (s) | ratio |
|---|---|---|---|---|---|
| key cardinality | 10 | 1 | 6.6 | 6.5 | 1.02 |
| key cardinality | 1000 | 1 | 6.2 | 7.5 | 0.83 |
| key cardinality | 100000 | 1 | 7.8 | 10.2 | 0.76 |
| key cardinality | 1000000 (base) | 1 | 11.2 | 15.4 | 0.72 |
| key cardinality | 10000000 | 1 | 23.0 | 25.5 | 0.90 |
| key cardinality | 1000000000 | 1 | 38.5 | 32.5 | 1.18 |
| skew | 0p0 (base) | 1 | 11.2 | 15.4 | 0.72 |
| skew | 0p8 | 1 | 11.0 | 14.1 | 0.78 |
| skew | 1p2 | 1 | 10.6 | 13.2 | 0.80 |
| clustering | random (base) | 1 | 11.2 | 15.4 | 0.72 |
| clustering | sorted | 1 | 7.5 | 8.3 | 0.90 |
| clustering | overlap | 1 | 7.6 | 9.2 | 0.82 |
| files | 4 | 1 | 12.4 | 14.9 | 0.83 |
| files | 32 (base) | 1 | 11.2 | 15.4 | 0.72 |
| files | 256 | 1 | 11.9 | 21.1 | 0.56 |
| null fraction | 0p0 (base) | 1 | 11.2 | 15.4 | 0.72 |
| null fraction | 0p2 | 1 | 11.0 | 15.2 | 0.72 |
| null fraction | 0p9 | 1 | 10.9 | 15.4 | 0.71 |
| key type | int (base) | 1 | 11.2 | 15.4 | 0.72 |
| key type | string | 1 | 15.9 | 21.5 | 0.74 |

One axis changed at a time from the base dataset (k=1000000, no skew, random, 32 files, no nulls, int key); schemas that change two axes at once are not in this table.

## Knob sweep (Flight): cells (schema, query) where a configuration beats default by more than 15% and more than 0.2 s

| configuration | faster | slower | median change |
|---|---|---|---|
| bcast_10m | 7 | 39 | +10% |
| bcast_256m | 15 | 16 | -1% |
| bloom_off | 13 | 10 | -5% |
| cbo_off | 9 | 74 | +21% |
| cbo_reorder_off | 15 | 15 | -6% |
| complete_agg_off | 14 | 17 | -4% |
| partial_decimal_sum_on | 9 | 26 | +8% |
| retain_off | 19 | 16 | -5% |
| reuse_bcast_only | 13 | 16 | -6% |
| rtf_all | 11 | 13 | -3% |
| shuffle_192 | 8 | 19 | -1% |
| shuffle_48 | 18 | 10 | -5% |

176 cells in total. Cells where the best configuration beats default by more than 15% and 0.2 s:

| query | schema | default (s) | best (s) | configuration |
|---|---|---|---|---|
| count_distinct_sel50 | k10_s0p0_random_f32_n0p0_int | 3.08 | 0.54 | retain_off |
| join_selective_dim | k10_s0p0_random_f32_n0p0_int | 1.03 | 0.21 | retain_off |
| join_agg_same_key | k1000_s1p2_overlap_f32_n0p0_int | 3.29 | 0.67 | shuffle_48 |
| filter_groupby_sel50 | k1000000_s0p0_random_f32_n0p2_int | 3.21 | 0.67 | retain_off |
| filter_groupby_sel50 | k1000000_s1p2_overlap_f32_n0p0_int | 3.27 | 0.72 | cbo_reorder_off |
| filter_groupby_sel1 | k1000_s1p2_random_f32_n0p0_int | 3.24 | 0.72 | reuse_bcast_only |
| topn | k1000_s1p2_overlap_f32_n0p0_int | 3.31 | 0.86 | shuffle_48 |
| join_selective_dim | k1000_s0p0_sorted_f32_n0p0_int | 3.61 | 0.96 | complete_agg_off |
| join_selective_dim | k1000_s1p2_random_f32_n0p0_int | 3.52 | 0.95 | reuse_bcast_only |
| filter_groupby_sel50 | k10000000_s0p0_random_f32_n0p0_int | 1.23 | 0.67 | retain_off |
| filter_groupby_sel1 | k1000_s1p2_overlap_f32_n0p0_int | 1.26 | 0.72 | shuffle_48 |
| join_selective_dim | k100000_s0p0_random_f32_n0p0_int | 1.78 | 1.09 | shuffle_48 |
| join_selective_dim | k1000000_s0p0_random_f32_n0p2_int | 1.84 | 1.16 | shuffle_48 |
| count_distinct_sel1 | k100000_s0p0_random_f32_n0p0_int | 0.83 | 0.55 | retain_off |
| join_selective_dim | k1000000_s0p0_random_f32_n0p0_int | 1.56 | 1.20 | shuffle_48 |
| join_selective_dim | k1000000_s0p8_random_f32_n0p0_int | 1.52 | 1.17 | shuffle_48 |
| join_selective_dim | k1000000_s1p2_sorted_f32_n0p0_int | 1.39 | 1.09 | shuffle_48 |
| join_selective_dim | k1000000_s1p2_random_f32_n0p0_int | 1.61 | 1.27 | shuffle_48 |
| join_selective_dim | k1000000_s0p0_random_f256_n0p0_int | 1.65 | 1.32 | shuffle_48 |
| join_selective_dim | k1000000_s0p0_overlap_f32_n0p0_int | 1.37 | 1.09 | retain_off |
| join_selective_dim | k1000_s1p2_sorted_f32_n0p0_int | 1.17 | 0.97 | complete_agg_off |
| join_selective_dim | k1000000_s0p0_random_f4_n0p0_int | 1.44 | 1.20 | retain_off |
| join_agg_same_key | k1000000_s1p2_sorted_f32_n0p0_int | 1.46 | 1.23 | retain_off |
| join_agg_same_key | k1000000_s0p0_sorted_f32_n0p0_int | 1.53 | 1.29 | reuse_bcast_only |

24 such cells of 176.
