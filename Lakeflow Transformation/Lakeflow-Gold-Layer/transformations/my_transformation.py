import dlt
import pyspark.sql.functions as F

# ==============================================================================
# GOLD DIMENSIONS
# ==============================================================================

@dlt.table(
    name="dim_passengers",
    comment="Dimension table for passenger details."
)
def dim_passengers():
    return dlt.read("flights_catalog.silver.silver_passengers").select(
        "passenger_id", 
        "name", 
        "gender", 
        "nationality"
    )

@dlt.table(
    name="dim_airports",
    comment="Dimension table for airport locations."
)
def dim_airports():
    return dlt.read("flights_catalog.silver.silver_airports").select(
        "airport_id", 
        "airport_name", 
        "city", 
        "country"
    )

@dlt.table(
    name="dim_flights",
    comment="Dimension table for flight schedules and routes."
)
def dim_flights():
    return dlt.read("flights_catalog.silver.silver_flights").select(
        "flight_id", 
        "airline", 
        "origin", 
        "destination", 
        "flight_date"
    )

# ==============================================================================
# GOLD FACT (The "Center" of the Star)
# ==============================================================================

@dlt.table(
    name="fact_bookings",
    comment="Fact table containing transactional booking events and revenue.",
    partition_cols=["booking_date"]
)
def fact_bookings():
    # Read the Silver bookings table
    df_bookings = dlt.read("flights_catalog.silver.silver_bookings")
    
    # Filter for the active records (because you used SCD Type 2 in Silver!)
    active_bookings = df_bookings.filter(F.col("__END_AT").isNull())

    return active_bookings.select(
        "booking_id",
        "flight_id",      
        "passenger_id",   
        "booking_date",   
        "amount"          
    )