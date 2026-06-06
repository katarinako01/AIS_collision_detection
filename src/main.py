import os
import sys
import glob
import zipfile
import math
import folium
from folium import plugins
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel
import h3

# ── 1. SETUP AND SPARKSESSION ───────────────────────────────

# ------------- Data directory and zip extraction -------------
# Supports two modes:
#   1. Place extracted CSVs directly in /data/
#   2. Place the zip archive in /data/ — pipeline extracts automatically

DATA_DIR = os.environ.get("DATA_DIR", "/data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

csv_files = glob.glob(os.path.join(DATA_DIR, "aisdk-2021-12-*.csv"))
zip_files = glob.glob(os.path.join(DATA_DIR, "*.zip"))

if csv_files:
    print(f"Found {len(csv_files)} CSV file(s) — skipping extraction.")
elif zip_files:
    print(f"No CSVs found — extracting {zip_files[0]}...")
    with zipfile.ZipFile(zip_files[0], "r") as z:
        z.extractall(DATA_DIR)
    csv_files = glob.glob(os.path.join(DATA_DIR, "aisdk-2021-12-*.csv"))
    print(f"Extraction complete. {len(csv_files)} CSV file(s) available.")
else:
    print(f"ERROR: No CSV or ZIP files found in {DATA_DIR}")
    sys.exit(1)

DATA_PATH = os.path.join(DATA_DIR, "aisdk-2021-12-*.csv")

spark = SparkSession.builder \
    .appName("AIS Collision Detection") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50mb") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()

# ── 2. DATA LOADING AND CLEANING ───────────────────────────

# ------------- Data loading -------------
# Schema defined upfront -> avoids inferSchema double scan of all CSVs
# Column names assigned positionally, so '# Timestamp' header is mapped
# cleanly to 'Timestamp' so no need of withColumnRenamed

schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("TypeOfMobile", StringType(), True),
    StructField("MMSI", LongType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("NavigationalStatus", StringType(), True),
    StructField("ROT", DoubleType(), True),
    StructField("SOG", DoubleType(), True),
    StructField("COG", DoubleType(), True),
    StructField("Heading", DoubleType(), True),
    StructField("IMO", StringType(), True),
    StructField("Callsign", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("ShipType", StringType(), True),
    StructField("CargoType", StringType(), True),
    StructField("Width", DoubleType(), True),
    StructField("Length", DoubleType(), True),
    StructField("TypeOfPositionFixingDevice", StringType(), True),
    StructField("Draught", DoubleType(), True),
    StructField("Destination", StringType(), True),
    StructField("ETA", StringType(), True),
    StructField("DataSourceType", StringType(), True),
    StructField("A", DoubleType(), True),
    StructField("B", DoubleType(), True),
    StructField("C", DoubleType(), True),
    StructField("D", DoubleType(), True),
])

df = spark.read.csv(DATA_PATH, header=True, schema=schema)

# ------------- CONSTANTS -------------

# H3 spatial index resolution. Resolution 8 gives hexagons of ~0.5km diameter,
# appropriate for collision detection without over-fragmenting the search space
H3_RESOLUTION = 8

# Maximum distance between two vessels to be considered a collision candidate.
# 0.05nm ≈ 90 meters. Tight enough to exclude near-misses and crossing vessels.
# Based on Martelli et al. (2024) collision zone radius of 5L for commercial vessels
COLLISION_THRESHOLD_NM = 0.05

# Maximum plausible vessel speed. Anything above this in the SOG field
# or implied from consecutive positions is treated as a GPS anomaly
MAX_SOG = 50.0

# Minimum speed to be considered a moving vessel. Filters out vessels
# that are effectively stationary but not reporting anchor/moored status
MIN_SOG = 2.0

# Earth radius in nautical miles -> used in Haversine distance formula.
# Derived from standard Earth radius 6371km / 1.852km per nautical mile
EARTH_RADIUS_NM = 3440.065

# Center coordinate for the 50nm geographic filter
CENTER_LAT = 55.225000
CENTER_LON = 14.245000

# Geographic filter radius in nautical miles
RADIUS_NM = 50.0

# Bounding box margins for fast pre-filter before Haversine
# Slightly larger than 50nm circle to avoid edge clipping
GEO_LAT_DELTA = 0.8
GEO_LON_DELTA = 1.2

# Minimum speed differential between vessel pair -> one vessel catching up
# to another. Filters out convoy/fleet pairs moving at similar speeds
MIN_SOG_DIFF = 2.0

# Minimum speed of faster vessel in pair -> ensures at least one vessel
# is moving at significant speed
MIN_SOG_MAX = 5.0

# Maximum number of distinct minute buckets a pair can appear in
# Real collision: 1-2 buckets maximum
# Convoy/fleet: multiple consecutive buckets
MAX_PAIR_BUCKETS = 2

# below: post-detection verification constants
MAX_COLLISION_DURATION_SEC = 30 # seconds; instantaneous collision threshold
MIN_PING_COUNT = 2 # exclude single-ping GPS coincidences
MAX_PRE_EVENT_COG_STDDEV = 2.0 # degrees; straight trajectory threshold
PRE_EVENT_WINDOW_MIN = 10 # mins to look back before event

# ── 3. GEOGRAPHIC AND STATIONARY FILTERING ─────────────────

# ------------- Parse timestamp -------------

df = df.withColumn(
    "Timestamp",
    F.to_timestamp(F.col("Timestamp"), "dd/MM/yyyy HH:mm:ss")
)

# ------------- TypeOfMobile filter -> keep only vessels, not base stations -------------

df = df.filter(F.col("TypeOfMobile").isin(["Class A", "Class B"]))

# ------------- MMSI validity filter (ITU-R M.585-9) -------------
# Excludes test MMSIs, navigational aids, SAR aircraft, AtoN, coast stations

df = df.filter(
    F.col("MMSI").isNotNull() &
    # ITU-R M.585-9: valid MMSI should 9 digits
    (F.col("MMSI") >= 100000000) &
    (F.col("MMSI") <= 999999999) &
    # exclude repeated digit patterns, e.g. test/invalid MMSIs
    ~((F.col("MMSI") % 111111111) == 0) &
    # exclude known test MMSI
    (F.col("MMSI") != 123456789) &
    # exclude navigational aids 970xxxxxx and AtoN 990xxxxxx-999xxxxxx
    ~((F.col("MMSI") >= 970000000) & (F.col("MMSI") <= 999999999)) &
    #exclude SAR aircraft 111xxxxxx
    ~((F.col("MMSI") >= 111000000) & (F.col("MMSI") <= 111999999))
)

# ------------- coordinate validity filter -------------

df = df.filter(
    (F.col("Latitude").between(-90, 90)) &
    (F.col("Longitude").between(-180, 180)) &
    F.col("Latitude").isNotNull() &
    F.col("Longitude").isNotNull()
)

# ------------- GPS jump filter, implied speed between consecutive positions -------------
# purpose: catches coordinate teleportation that SOG field alone won't catch
# per Zhang et al. (2023): AIS errors occur during collection, transmission, reception

speed_window = Window.partitionBy("MMSI").orderBy("Timestamp")

df = df.withColumn("prev_lat", F.lag("Latitude", 1).over(speed_window)) \
       .withColumn("prev_lon", F.lag("Longitude", 1).over(speed_window)) \
       .withColumn("prev_ts", F.lag("Timestamp", 1).over(speed_window)) \
       .withColumn("time_diff_hrs",
           (F.unix_timestamp("Timestamp") - F.unix_timestamp("prev_ts")) / 3600
       ).withColumn("implied_speed_nm",
           F.when(F.col("time_diff_hrs") > 0,
               F.acos(
                   F.sin(F.radians(F.col("prev_lat"))) * F.sin(F.radians(F.col("Latitude"))) +
                   F.cos(F.radians(F.col("prev_lat"))) * F.cos(F.radians(F.col("Latitude"))) *
                   F.cos(F.radians(F.col("Longitude")) - F.radians(F.col("prev_lon")))
               ) * F.lit(EARTH_RADIUS_NM) / F.col("time_diff_hrs")
           ).otherwise(None)
       ).filter(
           F.col("implied_speed_nm").isNull() |
           (F.col("implied_speed_nm") <= MAX_SOG)
       ).drop("prev_lat", "prev_lon", "prev_ts", "time_diff_hrs", "implied_speed_nm")

# ------------- Fast bounding box pre-filter -------------
# purpose: does arithmetic comparison before computationally expensive Haversine calculation
# margins slightly larger than 50nm circle to avoid boundary clipping

df = df.filter(
    (F.col("Latitude").between(CENTER_LAT - GEO_LAT_DELTA, CENTER_LAT + GEO_LAT_DELTA)) &
    (F.col("Longitude").between(CENTER_LON - GEO_LON_DELTA, CENTER_LON + GEO_LON_DELTA))
)

# ------------- Haversine distance from center point  -------------
# purpose: precise circular geographic filter after bbox pre-filter

df = df.withColumn("dist_from_center",
    F.acos(
        F.sin(F.radians(F.lit(CENTER_LAT))) * F.sin(F.radians(F.col("Latitude"))) +
        F.cos(F.radians(F.lit(CENTER_LAT))) * F.cos(F.radians(F.col("Latitude"))) *
        F.cos(F.radians(F.col("Longitude")) - F.radians(F.lit(CENTER_LON)))
    ) * F.lit(EARTH_RADIUS_NM)
)

df_filtered = df.filter(F.col("dist_from_center") <= RADIUS_NM)

# ------------- Stationary vessel filter -------------
# Dual criteria: SOG threshold + navigational status
# SOG > 0.5 knots: maritime threshold for moving vessel (standard)
# Status exclusions: vessels self-reporting as stationary

STATIONARY_STATUSES = [
    "At anchor",
    "Moored",
    "Aground",
    "Not under command"
]

df_filtered = df_filtered.filter(
    (F.col("SOG") > 0.5) &
    (~F.col("NavigationalStatus").isin(STATIONARY_STATUSES))
)

# ------------- Cache filtered dataset -------------
# purposeL to avoid recomputing the full pipeline twice

df_filtered.cache()

print("Row count after all filtering:")
print(df_filtered.count())

# ── 4. COLLISION CANDIDATE DETECTION (H3 + DCPA/TCPA) ──────────────

# ------------- H3 UDFs -------------
# Purpose: assigns H3 hexagon cell ID at resolution 8 ( approx 0.5km) to each vessel position.
# Used for spatial bucketing to avoid a full Cartesian join.
@F.udf(StringType())
def get_h3_cell(lat, lon):
    if lat is None or lon is None:
        return None
    return h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

# Purpose: returns the H3 cell and its 6 neighbors (k=1 ring).
# Ensures vessels near hexagon boundaries are not missed
@F.udf(ArrayType(StringType()))
def get_h3_neighbors(lat, lon):
    if lat is None or lon is None:
        return None
    cell = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
    return list(h3.grid_disk(cell, 1))

# ------------- Time bucketing -------------
# Purpose: bins pings into 1-minute windows -> AIS Class A transmits every 2-10s
# so multiple pings fall in each bucket per Zhang et al. (2023)

df_filtered = df_filtered.withColumn(
    "TimeBucket",
    F.date_trunc("minute", F.col("Timestamp"))
)

# ------------- H3 spatial indexing -------------
# Purpose: assigns hexagonal cell to each position — uniform edge distances vs
# rectangular grid, reducing boundary artifacts

df_filtered = df_filtered.withColumn(
    "h3_cell",
    get_h3_cell(F.col("Latitude"), F.col("Longitude"))
).filter(F.col("h3_cell").isNotNull())

# Explode to cell + 6 neighbors -> ensures pairs straddling hex boundaries
# are not missed
df_exploded = df_filtered.withColumn(
    "h3_neighbors",
    get_h3_neighbors(F.col("Latitude"), F.col("Longitude"))
).withColumn(
    "h3_join_key",
    F.explode(F.col("h3_neighbors"))
).drop("h3_neighbors")

# ------------- Repartition before join -> to reduce shuffle overhead -------------
# Purpose: co-locates matching keys on same partition before self-join

df_exploded = df_exploded.repartition(200, "h3_join_key", "TimeBucket")

# ------------- Persist exploded dataframe -------------
# Used twice in self-join (as both a and b) -> persisting avoids recomputation

df_exploded.persist(StorageLevel.MEMORY_AND_DISK)

# ------------- Self join on time bucket + H3 cell -------------
# Only compares vessels in same minute bucket and same hexagonal cell
# MMSI_A < MMSI_B ensures each pair appears once

df_a = df_exploded.alias("a")
df_b = df_exploded.alias("b")

candidates = df_a.join(
    df_b,
    (F.col("a.TimeBucket") == F.col("b.TimeBucket")) &
    (F.col("a.h3_join_key") == F.col("b.h3_join_key")) &
    (F.col("a.MMSI") < F.col("b.MMSI")),
    how="inner"
)

# ------------- Haversine distance between candidate pairs -------------
# Computed only on candidates, not the full dataset

candidates = candidates.withColumn("vessel_distance_nm",
    F.acos(
        F.sin(F.radians(F.col("a.Latitude"))) * F.sin(F.radians(F.col("b.Latitude"))) +
        F.cos(F.radians(F.col("a.Latitude"))) * F.cos(F.radians(F.col("b.Latitude"))) *
        F.cos(F.radians(F.col("b.Longitude")) - F.radians(F.col("a.Longitude")))
    ) * F.lit(EARTH_RADIUS_NM)
)

# ------------- DCPA/TCPA calculation -------------
# Distance and Time at Closest Point of Approach
# per Zhang et al. (2023) -> more predictive than instantaneous distance
# Positive TCPA = vessels still approaching
# Negative TCPA = vessels already past closest point
# DCPA small + TCPA positive = genuine collision risk

NM_PER_DEG_LAT = 60.0  # 1 degree latitude = 60nm

candidates = candidates \
    .withColumn("dvx",  # relative velocity east component (nm/hr)
        F.col("b.SOG") * F.sin(F.radians(F.col("b.COG"))) -
        F.col("a.SOG") * F.sin(F.radians(F.col("a.COG")))
    ).withColumn("dvy",  # relative velocity north component (nm/hr)
        F.col("b.SOG") * F.cos(F.radians(F.col("b.COG"))) -
        F.col("a.SOG") * F.cos(F.radians(F.col("a.COG")))
    ).withColumn("dx",  # relative position east (nm)
        (F.col("b.Longitude") - F.col("a.Longitude")) *
        F.lit(NM_PER_DEG_LAT) * F.cos(F.radians((F.col("a.Latitude") + F.col("b.Latitude")) / 2))
    ).withColumn("dy",  # relative position north (nm)
        (F.col("b.Latitude") - F.col("a.Latitude")) * F.lit(NM_PER_DEG_LAT)
    ).withColumn("rel_speed_sq",
        F.col("dvx") * F.col("dvx") + F.col("dvy") * F.col("dvy")
    ).withColumn("TCPA",
        F.when(F.col("rel_speed_sq") > 0,
            -(F.col("dx") * F.col("dvx") + F.col("dy") * F.col("dvy")) /
            F.col("rel_speed_sq")
        ).otherwise(F.lit(0.0))
    ).withColumn("DCPA",
        F.sqrt(
            F.pow(F.col("dx") + F.col("dvx") * F.col("TCPA"), 2) +
            F.pow(F.col("dy") + F.col("dvy") * F.col("TCPA"), 2)
        )
    ).drop("dvx", "dvy", "dx", "dy", "rel_speed_sq")

# ------------- Select and rename cols -------------

collisions = candidates.filter(
    (F.col("vessel_distance_nm") <= COLLISION_THRESHOLD_NM) &
    (F.col("a.SOG") <= MAX_SOG) &
    (F.col("b.SOG") <= MAX_SOG)
).select(
    F.col("a.MMSI").alias("MMSI_A"),
    F.col("b.MMSI").alias("MMSI_B"),
    F.col("a.Timestamp").alias("Timestamp"),
    F.col("a.TimeBucket").alias("TimeBucket"),
    F.col("a.Latitude").alias("Lat_A"),
    F.col("a.Longitude").alias("Lon_A"),
    F.col("b.Latitude").alias("Lat_B"),
    F.col("b.Longitude").alias("Lon_B"),
    F.col("a.SOG").alias("SOG_A"),
    F.col("b.SOG").alias("SOG_B"),
    F.col("a.COG").alias("COG_A"),
    F.col("b.COG").alias("COG_B"),
    F.col("a.Heading").alias("HDG_A"),
    F.col("b.Heading").alias("HDG_B"),
    F.col("a.Name").alias("Name_A"),
    F.col("b.Name").alias("Name_B"),
    F.col("a.ShipType").alias("ShipType_A"),
    F.col("b.ShipType").alias("ShipType_B"),
    F.col("a.Length").alias("Length_A"),
    F.col("b.Length").alias("Length_B"),
    F.col("vessel_distance_nm"),
    F.col("TCPA"),
    F.col("DCPA")
)

# ------------- Deduplicate -------------

collisions = collisions.dropDuplicates(["MMSI_A", "MMSI_B", "Timestamp"])

# ------------- Filter false positives -------------
# Vessel type exclusions: operationally normal proximity patterns
# Speed filters: both moving, significant speed differential
# DCPA filter: vessels genuinely converging, not just momentarily close
# TCPA filter: positive TCPA means vessels still approaching at time of ping

collisions_clean = collisions.filter(
    (F.col("vessel_distance_nm") > 0.0) &
    (F.col("SOG_A") >= MIN_SOG) &
    (F.col("SOG_B") >= MIN_SOG) &
    (F.greatest(F.col("SOG_A"), F.col("SOG_B")) >= MIN_SOG_MAX) &
    (F.abs(F.col("SOG_A") - F.col("SOG_B")) >= MIN_SOG_DIFF) &
    # DCPA: projected closest approach distance must be within collision threshold
    (F.col("DCPA") <= COLLISION_THRESHOLD_NM) &
    # TCPA: vessels must be approaching (positive) or just past closest point
    (F.col("TCPA") >= -1.0)  # allow up to 1 hour past closest point
)

# ------------- filter sustained proximity using distinct time buckets -------------
# Real collision: 1-2 distinct minute buckets max
# Convoy/fleet: multiple consecutive minute buckets
# Per Martelli et al. (2024): collision is a point-in-time event

bucket_counts = collisions_clean.groupBy("MMSI_A", "MMSI_B") \
    .agg(F.countDistinct("TimeBucket").alias("distinct_buckets"))

collisions_clean = collisions_clean.join(
    bucket_counts, on=["MMSI_A", "MMSI_B"], how="inner"
).filter(F.col("distinct_buckets") <= MAX_PAIR_BUCKETS)

# ------------- results -------------

print("Collision candidates after all filters:")
print(collisions_clean.count())

# ── 5. POST-DETECTION VERIFICATION ───────────────────────

# ------------- Post-detection verification -------------
# Two physics-based filters applied after candidate detection:
# 1. Collision duration <= 30 seconds -> instantaneous contact vs sustained proximity
#    Based on physical nature of collision: vessels touch briefly then diverge
#    Operational proximity (boarding, escort) lasts minutes
# 2. Pre-event trajectory linearity -> at least one vessel on straight course
#    Based on COLREG Rule 7: constant bearing = collision course
#    Maneuvering vessels (pilot, SAR) show high COG variance before proximity

# ------------- 1: duration filter -------------
# Cheap groupBy on candidates — runs fast
# Cache collisions to avoid recomputation in subsequent steps

collisions.cache()

duration_check = collisions.groupBy("MMSI_A", "MMSI_B").agg(
    F.min("Timestamp").alias("first_ts"),
    F.max("Timestamp").alias("last_ts"),
    F.count("Timestamp").alias("ping_count"),
    (F.unix_timestamp(F.max("Timestamp")) -
     F.unix_timestamp(F.min("Timestamp"))).alias("duration_seconds"),
    F.min("vessel_distance_nm").alias("min_distance")
).filter(
    # instantaneous contact -> not sustained proximity
    (F.col("duration_seconds") <= MAX_COLLISION_DURATION_SEC) &
    # at least 2 pings -> eliminates single GPS coincidences
    (F.col("ping_count") >= MIN_PING_COUNT)
)

print("Candidates after duration filter:")
print(duration_check.count())

# ------------- 2: trajectory linearity filter -------------
# Only runs on duration survivors -> much smaller set than full candidates
# AQE will automatically decide broadcast vs shuffle based on available memory

duration_survivors = duration_check.select("MMSI_A", "MMSI_B", "first_ts").cache()

# Pre-event trajectory for vessel A
pre_a = duration_survivors.join(
    df_filtered.select(
        F.col("MMSI").alias("MMSI_A"),
        F.col("Timestamp").alias("pre_ts"),
        F.col("COG").alias("pre_COG_A")
    ),
    on="MMSI_A",
    how="inner"
).filter(
    (F.col("pre_ts") < F.col("first_ts")) &
    (F.col("pre_ts") >= F.col("first_ts") - F.expr(f"INTERVAL {PRE_EVENT_WINDOW_MIN} MINUTES"))
).groupBy("MMSI_A", "MMSI_B").agg(
    F.stddev("pre_COG_A").alias("cog_stddev_A")
)

# Pre-event trajectory for vessel B
pre_b = duration_survivors.join(
    df_filtered.select(
        F.col("MMSI").alias("MMSI_B"),
        F.col("Timestamp").alias("pre_ts"),
        F.col("COG").alias("pre_COG_B")
    ),
    on="MMSI_B",
    how="inner"
).filter(
    (F.col("pre_ts") < F.col("first_ts")) &
    (F.col("pre_ts") >= F.col("first_ts") - F.expr(f"INTERVAL {PRE_EVENT_WINDOW_MIN} MINUTES"))
).groupBy("MMSI_A", "MMSI_B").agg(
    F.stddev("pre_COG_B").alias("cog_stddev_B")
)

# ------------- Combine all filters -------------

collisions_verified = duration_check \
    .join(pre_a, on=["MMSI_A", "MMSI_B"], how="left") \
    .join(pre_b, on=["MMSI_A", "MMSI_B"], how="left") \
    .withColumn("min_cog_stddev",
        F.least(
            F.coalesce(F.col("cog_stddev_A"), F.lit(999.0)),
            F.coalesce(F.col("cog_stddev_B"), F.lit(999.0))
        )
    ).filter(
        F.col("min_cog_stddev") <= MAX_PRE_EVENT_COG_STDDEV
    )

# ------------- Join back to get full collision details -------------

final_result = collisions_verified.join(
    collisions_clean.select(
        "MMSI_A", "MMSI_B", "Name_A", "Name_B",
        "ShipType_A", "ShipType_B",
        "Lat_A", "Lon_A", "Lat_B", "Lon_B",
        "SOG_A", "SOG_B", "COG_A", "COG_B",
        "HDG_A", "HDG_B", "Length_A", "Length_B",
        "vessel_distance_nm", "TCPA", "DCPA"
    ).distinct(),
    on=["MMSI_A", "MMSI_B"],
    how="inner"
).orderBy("vessel_distance_nm")

print("Final verified collision candidates:")
print(final_result.count())

# ── 6. RESULTS AND VISUALISATION ───────────────────────────

# ------------- Get all verified collisions -------------

all_events = final_result.select(
    "MMSI_A", "MMSI_B", "first_ts", "Name_A", "Name_B",
    "Lat_A", "Lon_A", "Lat_B", "Lon_B",
    "vessel_distance_nm",
    "DCPA", "TCPA", "SOG_A", "SOG_B",
    "HDG_A", "HDG_B", "ShipType_A", "ShipType_B"
).dropDuplicates(["MMSI_A", "MMSI_B"]) \
 .orderBy("vessel_distance_nm") \
 .collect()

print(f"Total collisions detected: {len(all_events)}")

# ------------- COLREG classification function -------------

def classify_colreg(lat_a, lon_a, hdg_a, lat_b, lon_b):
    """Classify collision scenario per COLREG Rules 13-15.
    Uses relative bearing of vessel B from vessel A's perspective.
    Rule 13 (Overtaking):relative bearing 112.5° to 247.5°
    Rule 14 (Head-on): relative bearing < 22.5° or > 337.5°
    Rule 15 (Crossing): all other cases
    """
    if hdg_a is None or lat_b is None:
        return "Unknown"
    dlon = math.radians(lon_b - lon_a)
    x = math.sin(dlon) * math.cos(math.radians(lat_b))
    y = math.cos(math.radians(lat_a)) * math.sin(math.radians(lat_b)) - \
        math.sin(math.radians(lat_a)) * math.cos(math.radians(lat_b)) * math.cos(dlon)
    abs_bearing = (math.degrees(math.atan2(x, y)) + 360) % 360
    rel_bearing = (abs_bearing - hdg_a) % 360
    if rel_bearing < 22.5 or rel_bearing > 337.5:
        return f"Head-on (Rule 14) -> relative bearing {rel_bearing:.1f}°"
    elif 112.5 <= rel_bearing <= 247.5:
        return f"Overtaking (Rule 13) -> relative bearing {rel_bearing:.1f}°"
    else:
        return f"Crossing (Rule 15) -> relative bearing {rel_bearing:.1f}°"

# ------------- Process each collision -------------

for i, collision_event in enumerate(all_events):

    COLLISION_MMSI_A = collision_event["MMSI_A"]
    COLLISION_MMSI_B = collision_event["MMSI_B"]
    COLLISION_TIME = str(collision_event["first_ts"])
    VESSEL_NAME_A = collision_event["Name_A"] or f"MMSI {COLLISION_MMSI_A}"
    VESSEL_NAME_B = collision_event["Name_B"] or f"MMSI {COLLISION_MMSI_B}"
    collision_lat = collision_event["Lat_A"]
    collision_lon = collision_event["Lon_A"]

    scenario = classify_colreg(
        collision_event["Lat_A"], collision_event["Lon_A"], collision_event["HDG_A"],
        collision_event["Lat_B"], collision_event["Lon_B"]
    )

    # ------------- results -------------
    print(f"\n{'-'*60}")
    print(f"COLLISION {i+1} OF {len(all_events)}")
    print(f"{'-'*60}")
    print(f"Vessel A:        {VESSEL_NAME_A} (MMSI: {COLLISION_MMSI_A})")
    print(f"Vessel B:        {VESSEL_NAME_B} (MMSI: {COLLISION_MMSI_B})")
    print(f"Timestamp:       {COLLISION_TIME} UTC")
    print(f"Location:        {collision_event['Lat_A']:.6f}°N, {collision_event['Lon_A']:.6f}°E")
    print(f"Distance:        {collision_event['vessel_distance_nm']*1852:.1f} meters")
    print(f"DCPA:            {collision_event['DCPA']*1852:.1f} meters")
    print(f"TCPA:            {collision_event['TCPA']*60:.1f} minutes")
    print(f"SOG A:           {collision_event['SOG_A']} knots")
    print(f"SOG B:           {collision_event['SOG_B']} knots")
    print(f"Ship Type A:     {collision_event['ShipType_A']}")
    print(f"Ship Type B:     {collision_event['ShipType_B']}")
    print(f"COLREG Scenario: {scenario}")
    print(f"{'-'*60}")

    # ------------- Extract trajectory ±10 minutes -------------
    trajectory = df_filtered.filter(
        (F.col("MMSI").isin([COLLISION_MMSI_A, COLLISION_MMSI_B])) &
        (F.col("Timestamp") >= F.lit(collision_event["first_ts"]) - F.expr("INTERVAL 10 MINUTES")) &
        (F.col("Timestamp") <= F.lit(collision_event["first_ts"]) + F.expr("INTERVAL 10 MINUTES"))
    ).select("MMSI", "Name", "Timestamp", "Latitude", "Longitude", "SOG", "COG") \
     .orderBy("Timestamp")

    traj_pd = trajectory.toPandas()
    traj_a = traj_pd[traj_pd["MMSI"] == COLLISION_MMSI_A].sort_values("Timestamp").reset_index(drop=True)
    traj_b = traj_pd[traj_pd["MMSI"] == COLLISION_MMSI_B].sort_values("Timestamp").reset_index(drop=True)

    # ------------- Build Folium map -------------
    m = folium.Map(
        location=[collision_lat, collision_lon],
        zoom_start=13,
        tiles="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        attr="CartoDB Voyager"
    )
    # nautical chart overlay
    folium.TileLayer(
        tiles="https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png",
        attr="OpenSeaMap",
        name="Nautical",
        overlay=True,
        opacity=0.7
    ).add_to(m)

    folium.LayerControl().add_to(m)

    karin_coords = list(zip(traj_a["Latitude"], traj_a["Longitude"]))
    scot_coords = list(zip(traj_b["Latitude"], traj_b["Longitude"]))

    folium.PolyLine(karin_coords, color="#d62728", weight=3,
                    opacity=0.8, tooltip=f"{VESSEL_NAME_A} trajectory").add_to(m)
    folium.PolyLine(scot_coords, color="#1f77b4", weight=3,
                    opacity=0.8, tooltip=f"{VESSEL_NAME_B} trajectory").add_to(m)

    folium.Marker(karin_coords[0], popup=f"{VESSEL_NAME_A} — start (-10 min)",
                  icon=folium.Icon(color="red", icon="circle", prefix="fa")).add_to(m)
    folium.Marker(scot_coords[0], popup=f"{VESSEL_NAME_B} — start (-10 min)",
                  icon=folium.Icon(color="blue", icon="circle", prefix="fa")).add_to(m)
    folium.Marker(karin_coords[-1], popup=f"{VESSEL_NAME_A} — end (+10 min)",
                  icon=folium.Icon(color="darkred", icon="flag", prefix="fa")).add_to(m)
    folium.Marker(scot_coords[-1], popup=f"{VESSEL_NAME_B} — end (+10 min)",
                  icon=folium.Icon(color="darkblue", icon="flag", prefix="fa")).add_to(m)
    folium.Marker([collision_lat, collision_lon],
                  popup=f"Collision — {COLLISION_TIME} UTC",
                  icon=folium.Icon(color="orange", icon="warning-sign", prefix="glyphicon")).add_to(m)

    legend_html = f"""
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
         background-color: white; padding: 12px; border-radius: 6px;
         border: 1px solid #ccc; font-size: 13px;">
        <b>Vessel Collision — {COLLISION_TIME[:10]}</b><br>
        <span style="color:#d62728">●</span> {VESSEL_NAME_A} (MMSI: {COLLISION_MMSI_A})<br>
        <span style="color:#1f77b4">●</span> {VESSEL_NAME_B} (MMSI: {COLLISION_MMSI_B})<br>
        <span style="color:#ff7f0e">★</span> Collision point — {COLLISION_TIME} UTC
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # ------------- Save -------------
    output_path = f"{OUTPUT_DIR}/collision_map_{COLLISION_MMSI_A}_{COLLISION_MMSI_B}.html"
    m.save(output_path)
    print(f"Map saved: {output_path}")

spark.stop()