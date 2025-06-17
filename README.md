# Setup:
- Ensure you have docker desktop installed
- Clone the project repository:
  - SSH: `git clone git@github.com:hadasman/ZeroNetworksTest.git`
  - HTTPS: `https://github.com/hadasman/ZeroNetworksTest.git` 
- From the root directory run the command `docker compose build` and then `docker compose up -d` in the terminal
- Initialize the tables in Postgres:
    - Run `docker exec -it postgresql_db psql -U trino -d sampledb ` from terminal
    - In the Postgres CLI run:
```
CREATE TABLE spacex_launches (
    id TEXT PRIMARY KEY,
    name TEXT,
    launch_date_unix BIGINT,
    success BOOLEAN,
    details TEXT,
    payload_mass REAL,
    engine_start_time_unix BIGINT);
CREATE TABLE agg_spacex_launches (
    aggregation_year INTEGER PRIMARY KEY,
    total_launches BIGINT NOT NULL,
    total_successful_launches BIGINT NOT NULL,
    average_payload_mass REAL,
    average_delay_hours REAL
);
```

# Design
The pipeline consists of the following layers:
- Data ingestion (fetching the latest launch from the API)
- Data parsing and validation
  - Make sure mandatory columns are not null
  - Parse `details` field to extract total payload mass 
- Append new data to FACT table
- Fetch aggregated data using trino and upsert to aggregation table
  - Aggregation: since average is a non-linear metric it would need to be re-calculated with each row insertion, so I use 
    upsert in order to update these entries. Since the only incoming data is current and the aggregation is on a whole 
    year, I fetch only the last year for the purpose of this upsert (so in the example it will not upsert because the API has very old data).
  

# Assumptions
- All "details" messages are the same structure, when exist. I am disregarding successful launches with None details in the mass average.
- Failed launches are also counted in the calculation of mass average
- If no static_fire_date (sometimes None), delay = 0
- Aggregation is on the year part of launch date

# Testing
In order to execute the pipeline run the `__main__` part of the `src/main.py` file.
You may run the tests contained in the `tests` directory, in order to see how the pipeline handles various use cases.
Make sure you have the `unittest` package in stalled in order to run the tests.


Phase 6: Documentation and Deliverables
README.md:
Design Choices/Assumptions: Explain why you chose certain data types, how you handled incremental loading, your approach to aggregation (Python vs. SQL), and any assumptions made.

Limitations: Any known issues or areas for improvement if you had more time.