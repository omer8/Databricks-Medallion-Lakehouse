import dlt
import pyspark.sql.functions as F


# ==============================================================================
# SILVER — Bookings
# ==============================================================================

@dlt.view(name="bookings_bronze_stream")
@dlt.expect_or_drop("valid_booking_id", "booking_id IS NOT NULL")
@dlt.expect("valid_price", "amount >= 0")          
def bookings_bronze_stream():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")           
            .table("flights_catalog.bronze.bookings")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("booking_date", F.to_date(F.col("booking_date")))
        .drop("_rescued_data")
    )

dlt.create_streaming_table(
    name="silver_bookings",
    comment="Cleaned and deduplicated bookings with full change history.",
    partition_cols=["booking_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)

dlt.create_auto_cdc_flow(
    target="silver_bookings",
    source="bookings_bronze_stream",
    keys=["booking_id"],
    sequence_by=F.col("last_updated"),
    stored_as_scd_type="2",
    track_history_except_column_list=["last_updated","source_file"],     
)


# ==============================================================================
# SILVER — Passengers
# ==============================================================================

@dlt.view(name="passengers_bronze_stream")
@dlt.expect_or_drop("valid_passenger_id", "passenger_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
@dlt.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
def passengers_bronze_stream():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("flights_catalog.bronze.passengers")
        .withColumn("name",        F.initcap(F.trim(F.col("name"))))
        .withColumn("gender",      F.initcap(F.trim(F.col("gender"))))
        .withColumn("nationality", F.initcap(F.trim(F.col("nationality"))))
        .drop("_rescued_data")
    )

dlt.create_streaming_table(
    name="silver_passengers",
    comment="Cleaned passenger profiles. SCD1 — latest record only.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="passengers_bronze_stream",
    keys=["passenger_id"],
    sequence_by=F.col("last_updated"),
    stored_as_scd_type="1",                                
)


# ==============================================================================
# SILVER — Flights
# ==============================================================================

@dlt.view(name="flights_bronze_stream")
@dlt.expect_or_drop("valid_flight_id",   "flight_id IS NOT NULL")
@dlt.expect_or_drop("valid_flight_date", "flight_date IS NOT NULL")
@dlt.expect("valid_route", "origin != destination")
def flights_bronze_stream():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("flights_catalog.bronze.flights")
        .withColumn("airline",     F.initcap(F.trim(F.col("airline"))))
        .withColumn("origin",      F.initcap(F.trim(F.col("origin"))))
        .withColumn("destination", F.initcap(F.trim(F.col("destination"))))
        .withColumn("flight_date", F.to_date(F.col("flight_date")))
        .drop("_rescued_data")
    )

dlt.create_streaming_table(
    name="silver_flights",
    comment="Cleaned flight schedules. SCD1 — latest record only.",
    partition_cols=["flight_date"],
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="flights_bronze_stream",
    keys=["flight_id"],
    sequence_by=F.col("last_updated"),
    stored_as_scd_type="1",
)


# ==============================================================================
# SILVER — Airports  (reference table — no partitioning, SCD1)
# ==============================================================================

@dlt.view(name="airports_bronze_stream")
@dlt.expect_or_drop("valid_airport_id", "airport_id IS NOT NULL")
@dlt.expect_or_drop("valid_airport_name", "airport_name IS NOT NULL")
@dlt.expect_or_drop("valid_airport_city", "city IS NOT NULL")
@dlt.expect_or_drop("valid_airport_country", "country IS NOT NULL")
def airports_bronze_stream():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .table("flights_catalog.bronze.airports")
        .withColumn("airport_name", F.initcap(F.trim(F.col("airport_name"))))
        .withColumn("city",         F.initcap(F.trim(F.col("city"))))
        .withColumn("country",      F.initcap(F.trim(F.col("country"))))
        .drop("_rescued_data")
    )

dlt.create_streaming_table(
    name="silver_airports",
    comment="Standardized airport reference data. SCD1 — latest record only.",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    }
)

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="airports_bronze_stream",
    keys=["airport_id"],
    sequence_by=F.col("last_updated"),
    stored_as_scd_type="1",                                
)