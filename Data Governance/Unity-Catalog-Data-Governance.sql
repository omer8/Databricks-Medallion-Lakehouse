-- Give analysts the ability to see the catalog
GRANT USE CATALOG ON CATALOG flights_catalog TO `data_analysts`;

-- Give analysts the ability to see the gold schema
GRANT USE SCHEMA ON SCHEMA flights_catalog.gold TO `data_analysts`;

-- Give them permission to read all current tables in the gold schema
GRANT SELECT ON SCHEMA flights_catalog.gold TO `data_analysts`;

-- EXPLICIT DENY: Ensure they cannot see the raw, bronze, or silver layers
REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.raw FROM `data_analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.bronze FROM `data_analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA flights_catalog.silver FROM `data_analysts`;