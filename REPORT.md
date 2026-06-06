# AIS Vessel collision detection. Report

## 1. Introduction

This report describes the methodology, implementation and findings coming from a PySpark-based pipeline for detecting vessel collisions in Danish AIS data for December 2021. The analysis is restricted to a 50-nautical-mile radius around 55.225°N, 14.245°E, i.e. the Bornholmsgat Traffic Separation Scheme in the Baltic Sea.

AIS (Automatic Identification System) is a self-reporting system mandatory for vessels above 300 GT internationally. It transmits positional data including MMSI, timestamp, latitude, longitude, SOG, CO, and heading at intervals of 2–10 seconds for moving vessels (Liu et al., 2023). The full December 2021 dataset covers approximately 31 days of Baltic Sea traffic.

## 2. Data cleaning and noise filtering

Raw AIS data contains frequent errors at the collection, transmission and reception stages (Liu et al., 2023), hence the following filters were applied sequentially before collision detection:

**TypeOfMobile filter.** Only Class A and Class B vessel transponders are retained. Base stations and other non-vessel entries are excluded.

**MMSI validity (ITU-R M.585-9).** Valid MMSIs are 9-digit integers in the range 100000000–999999999.  Excluded range covers: navigational aids (970xxxxxx), AtoN (990xxxxxx–999xxxxxx), SAR aircraft (111xxxxxx), repeated-digit test MMSIs (111111111, 222222222, etc., detected via modulo arithmetic) and the known test MMSI 123456789.

**Coordinate validity.** Latitude must be within [−90, 90] and longitude within [−180, 180]. Null coordinates are discarded.

**GPS jump filter.** Implied speed between consecutive positions for the same MMSI is computed using the Haversine formula. Positions implying a speed exceeding 50 knots are removed. This filter catches coordinate teleportation that the SOG field alone cannot detect, as GPS anomalies may occur without triggering an abnormal SOG reading (Liu et al., 2023).

**Geographic filter.** A fast arithmetic bounding box pre-filter (+-0.8° latitude, +-1.2° longitude) eliminates approximately 90% of rows before the computationally expensive precise Haversine circle filter (50nm radius).

**Stationary vessel filter.** Vessels with SOG ≤ 0.5 knots or reporting navigational statuses of "At anchor", "Moored", "Aground" or "Not under command" are excluded. This dual-criterion approach handles both vessels that are stationary but transmitting a non-zero SOG as well as vessels that are technically moving but operationally immobile.

## 3. Spatial filtering and computational strategy

It should be noted that loading is optimised by pre-defining the CSV schema upfront, avoiding the double scan that Spark's inferSchema would otherwise perform to infer column types. Filtering is applied sequentially from cheapest to most expensive: TypeOfMobile and MMSI validity checks (simple comparisons) run first, followed by coordinate bounds, then the GPS jump filter (requires a window function over ordered partitions) and finally the geographic Haversine circle filter. This ordering ensures the most expensive operations operate on the smallest possible dataset.

Importantly, a naive self-join on the filtered dataset would produce hundreds of trillions of candidate pairs, which is computationally infeasible. The following strategies reduce complexity to O(n × k²), where k is the mean vessel count per spatiotemporal bucket (less than 3 in open Baltic water).

**H3 hexagonal spatial indexing.** Each vessel position is assigned to an H3 hexagonal cell at a resolution of 8 (~0.5km diameter). Hexagons are preferred over rectangular grids due to their uniform edge-to-center distances, which eliminate the geometric corner artifacts present in rectangular bucketing. The H3 cell assignment does use a Python UDF but it is justified by geometric correctness. Additionally, the overhead is minimal since it executes on the already-filtered dataset (approx. 17 million instead of the full raw dataset) and its result is persisted before the join.

**Temporal bucketing.** Positions are binned into 1-minute windows using `date_trunc`. AIS Class A transmits every 2–10 seconds, so multiple pings fall within each bucket (Liu et al., 2023). Only vessels in the same spatial cell and the same time window are being compared.

**Neighbor expansion (k=1 ring).** Each vessel's H3 cell is expanded to include its 6 immediate neighbours, ensuring that pairs straddling cell boundaries are not missed.

**Repartitioning before join.** The exploded dataset is repartitioned on `(h3_join_key, TimeBucket)` before the self-join, co-locating matching keys on the same executor partition and reducing shuffle overhead.

**Caching and persistence.** The filtered dataset is cached after preprocessing. The exploded dataset is persisted to `MEMORY_AND_DISK` before the self-join, which references it twice (as both sides of the join).

**Native Spark functions.** Haversine distance and DCPA/TCPA calculations use native Spark SQL functions throughout, allowing Catalyst optimizer to push down and combine operations without Python serialization overhead. The only UDF use case with the justification is discussed above.

On the combined effect of these optimisations: schema pre-definition avoids a double CSV scan on load. Early filtering reduces the full raw dataset to ~17 million rows before any join operation. Three intermediate results are explicitly cached or persisted (df_filtered, df_exploded and collisions) preventing Spark from recomputing the full pipeline for each downstream action. H3 bucketing then restricts comparisons to vessels sharing the same ~0.5km cell and 1-minute window, reducing candidate pairs from trillions to thousands. Haversine distance, DCPA and TCPA are computed only on these surviving candidates and not the full dataset. The post-detection verification filters (duration and trajectory linearity) operate on an even smaller set of ~233 pairs. The net result is a pipeline that completes on a single machine in under 90 minutes for a full month of Baltic Sea AIS data.

## 4. Collision detection

Candidate pairs (total: 17333847) surviving spatial and temporal bucketing are then evaluated using the following kinematic filters:

**Distance threshold.** Haversine distance between candidate pairs must be ≤ 0.05nm (~90 meters). This threshold is based on Martelli et al. (2024), who define a collision zone radius of 5L (five vessel lengths) for commercial vessels.

**DCPA/TCPA.** Distance and Time at Closest Point of Approach (accordingly: DCPA and TCPA) are computed from relative velocity vectors per Zhang et al. (2023). DCPA ≤ 0.05nm confirms vessels are on genuinely converging courses. TCPA ≥ −1.0 hours allows detection of pairs just past their closest point.

**Speed filters.** Both vessels must have SOG ≥ 2 knots (moving), the faster vessel must have SOG ≥ 5 knots and the speed differential must be ≥ 2 knots. This eliminates stationary pairs and vessels moving in formation at identical speeds.

**Distinct bucket count.** Pairs appearing in more than 2 distinct minute buckets are excluded. Real collisions are point-in-time events (Martelli et al., 2024), pairs appearing across many buckets indicate convoy or fleet proximity.

## 5. Post-detection verification

Two physics-based verification filters are then applied to the candidate set, i.e. 233 pairs surviving distance, speed, DCPA/TCPA and bucket count filters, requiring no vessel type knowledge:

**Collision duration filter.** For each candidate pair, the time span between the first and last ping within the distance threshold is computed. Pairs with duration > 30 seconds are excluded. This is grounded in the physical nature of a collision: two vessels moving at speed make contact briefly and almost immediately diverge. Operational proximity events, such as pilot boarding, escort, SAR operations, etc., involve sustained closeness lasting minutes (and in this dataset case - up to 3 days). Empirical validation on the full December dataset confirmed that KARIN HOEJ / MV SCOT CARRIER showed 20-second duration, while all operational false positives showed durations exceeding 2 minutes. Additionally, pairs with fewer than 2 pings within the threshold are excluded as single GPS coincidences.

**Pre-event trajectory linearity.** For each surviving pair, the standard deviation of COG over the 10-minute window before the event is computed for both vessels. Pairs where the minimum COG standard deviation across both vessels exceeds 2° are excluded. This is grounded in COLREG Rule 7 (Constant Bearing, Decreasing Range or CBDR): a vessel on a collision course maintains a constant bearing and straight trajectory because it is unaware of the danger or unable to maneuver in time. Maneuvering vessels (pilot boats, SAR, diving support) show high COG variance. KARIN HOEJ showed a pre-event COG standard deviation of 1.26°, confirming a straight-line approach consistent with CBDR.

## 6. Results

The pipeline further applies filters progressively, reducing the candidate set at each stage:

| Stage | Candidates |
|---|---|
| After kinematic filters | 233 pairs |
| After duration filter (≤30s, ≥2 pings) | 10 pairs |
| After trajectory linearity filter (COG stddev ≤2°) | 1 unique pair |

One collision event in December 2021 was identified:

```
Total collisions detected: 1

------------------------------------------------------------
COLLISION 1 OF 1
============================================================
Vessel A:        KARIN HOEJ (MMSI: 219021240)
Vessel B:        MV SCOT CARRIER (MMSI: 232018267)
Timestamp:       2021-12-13 02:27:09 UTC
Location:        55.223260°N, 14.244362°E
Distance:        65.1 meters
DCPA:            14.9 meters
TCPA:            0.2 minutes
SOG A:           6.1 knots
SOG B:           11.8 knots
Ship Type A:     Other
Ship Type B:     Cargo
COLREG Scenario: Crossing (Rule 15) -> relative bearing 251.1°
------------------------------------------------------------
```
This collision occurred in the Bornholmsgat Traffic Separation Scheme at 02:27 UTC. KARIN HOEJ was maintaining a steady southwest course (COG 222°, COG stddev 1.26°) at 6.1 knots. MV SCOT CARRIER was approaching from the northeast at 11.8 knots on a course of 269°. The relative bearing of MV SCOT CARRIER from KARIN HOEJ was 251.1°, placing the scenario marginally outside the COLREG Rule 13 overtaking arc (112.5°–247.5°) and classifying it as a Crossing scenario under Rule 15. The borderline nature of the classification (3.6° outside the overtaking threshold) suggests elements of both crossing as well as overtaking were present.

### Trajectory visualisation

The map screenshot below shows the geographic context of the collision, including the Bornholmsgat Traffic Separation Scheme lanes and the island of Bornholm:

![Geographic context — Bornholm and TSS lanes](images/map_context.png)

The detailed trajectory map screenshot shows both vessels' paths in the +-10 minute window around the collision. KARIN HOEJ (red) maintains a straight southwest trajectory throughout, whereas MV SCOT CARRIER (blue) makes a pronounced course change after the collision point, which is consistent with an emergency evasive manoeuvre:

![Vessel trajectories ±10 minutes around collision](images/map_detail.png)

## 7. Limitations

The pipeline detects collisions between AIS-equipped moving vessels within the specified area and timeframe. Some important limitations apply as following:

AIS transmission is mandatory only for vessels above 300 GT internationally, meaning smaller vessels are not present in the dataset. A collision involving an unequipped vessel would not be detectable.

AIS is self-reported and subject to transmission gaps. Vessels in distress may cease transmitting, GPS anomalies may corrupt positional data. While the implied-speed filter handles coordinate teleportation, transmission gaps remain undetectable by definition.

The minimum speed threshold (SOG ≥ 2 knots) excludes very slow-speed incidents near port entrances. The 50nm geographic constraint excludes events outside the specified area.

Martelli et al. (2024) and Zhang et al. (2023) both acknowledge that AIS-only collision detection systems should ideally be fused with radar data, satellite AIS and maritime incident databases for comprehensive coverage. Cross-referencing this result with documented maritime incident records for December 2021 confirms the identified event, validating the pipeline within its operational scope.

While this pipeline is fully automated and physics-grounded, it should be treated purely as a candidate detection system rather than a definitive accident classifier. All thresholds, including collision distance, duration, trajectory linearity, were empirically derived from the December 2021 dataset and validated against one confirmed incident. Their generalisability to other waterways, vessel traffic densities or time periods is not guaranteed. Users applying this pipeline to different datasets should verify that the threshold parameters remain appropriate for their context and cross-reference results against maritime incident databases where possible.

## 8. References

- Martelli, M., Žuškin, S., Cellerino, E., & Zaccone, R. (2024). Ship collision detection and classification employing AIS data. Paper presented at The 34th International Ocean and Polar Engineering Conference, Rhodes, Greece, June 2024. [ResearchGate](https://www.researchgate.net/publication/384980458_Ship_Collision_detection_and_classification_employing_AIS_data)
- Liu, Z., Zhang, B., Zhang, M., Wang, H., & Fu, X. (2023). A quantitative method for the analysis of ship collision risk using AIS data. *Ocean Engineering*, 272, 113906. [https://doi.org/10.1016/j.oceaneng.2023.113906](https://doi.org/10.1016/j.oceaneng.2023.113906)
- ITU-R M.585-9 (2019). Assignment and use of identities in the maritime mobile service. https://www.itu.int/rec/R-REC-M.585/en
- COLREG (1972). Convention on the International Regulations for Preventing Collisions at Sea. [https://www.imo.org/en/about/conventions/pages/colreg.aspx](https://www.imo.org/en/about/conventions/pages/colreg.aspx)
