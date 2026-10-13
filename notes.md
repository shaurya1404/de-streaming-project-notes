# Azure Databricks Streaming Project

## Overview

An end-to-end data engineering pipeline on real user activity from Apex - my Fitness Streak Tracker App live on the app store I built as a solo developer. For Analytics purposes, the app already sends events based on real user activity to PostHog. The data is now forwarded directly to Azure Event Hubs where the data pipeline initiates. PostHog uses an HTTP Webhook and communicates the Events in near real-time via a REST API (since PostHog doesn't offer a Native connection to Azure yet). From the hub, a Spark Declarative Pipeline will process the stream through bronze, silver and gold layers: deduplicating on event ID and pairing start and outcome events with stateful logic. The result is a star schema with separate fact tables for workouts, gym check-ins, social activity, onboarding and app reliability. Alongside the stream, Azure Data Factory will run the batch side: scheduled incremental loads of user, workout and gym tables from the app's database to serve as dimensions, and a backfill of historical events. Together they show how streaming facts and batch dimensions combine in one lakehouse model.

1) Event Hub

Event Hub is Azure's managed service for streaming events: a highly-scalable, append-only log that acts as the storage buffer between apps producing events and others consuming it. It is Azure's equivalent of Apache Kafka.

It allows Decoupling since the Producers and Consumers don't need to know about each other

- Step 1: Create Resource Group

- Step 2: Create Event Hub Namespace

- Step 3: Create Event Hub Instance

## Event Hub Namespace vs Instance

Namespace is the top-level container that has a unique address `<namespace>.servicebus.windows.net`. It consists of one or more instances.
The Namespace includes configurations like Region, Pricing tier, Capacity (Throughput units) that are shared among all the instances. 
An Instance includes configurations unique to the stream such as Retention period and Partition count

-  Step 4: Create Access Policy: In the Event Hub Instance, create a Send-Only Access Policy such that Posthog API requests can be autheticated and accepted into the Event Hub instance as a producer. 

- Step 5: Generate a Shared Access Signature (SAS) using the Primary key given by the Send Policy using the following Python script since the Portal gives us the Key, not the entire Authorization token needed by the Webhook:

```python
import time, hmac, hashlib, base64, urllib.parse

uri = "https://<namespace>.servicebus.windows.net/<instance>"
key_name, key = "<policy-name>", "<primary-key>"
expiry = int(time.time()) + 365 * 24 * 3600  # 1 year

sr = urllib.parse.quote_plus(uri)
sig = base64.b64encode(
    hmac.new(key.encode(), f"{sr}\n{expiry}".encode(), hashlib.sha256).digest()
)
print(f"SharedAccessSignature sr={sr}&sig={urllib.parse.quote_plus(sig)}&se={expiry}&skn={key_name}") # token
```

- Step 6: Webhook Config in PostHog
    Webhook Endpoint: `https://<namespace>.servicebus.windows.net/<eventhub>/messages`
    HTTP Method-Type: POST
    Authorization Header: <token>
    Content-Type Token: `application/json`

- Step 7: Defining the Payload Template in PostHog

Define the flat JSON template that includes fields such as `uuid`, `distinct_id`, `gym_slot`, `total_steps`, `workout_name`. So, each event is data stored in the payload of the request in the form of a serialized JSON String.

## Azure Data Factory

- Step 8: Azure Data Factory Initialization

ADF is Azure's managed service for data orchestration between systems. It's used for incremental batch ingestion of data from a database or SaaS application into a Data Lakehouse - thus, it complements the Event Hub architecture as one streams data in near real-time into the Bronze layer and the other incrementally batch loads data into the Bronze layer on a schedule.

ADF will be used in this project to:
(1) batch ingest all historical events that currently exist within PostHog before stream was implemented and 
(2) to ingest dimensions data on a schedule into the Lakehouse that will be joined with the facts data (streamed events). The streamed events currently only consists of factual data - thus, in order to enrich the data and give it more context, it will be joined to all the dimensional data pulled from the PostgreSQL database via ADF.

### Author Tab

The Author tab is where you build things in Azure Data Factory, and its Factory Resources panel lists the types of objects you can build:

1) Pipeline: A set of activities connected in order, with rules for what happens on success or failure. An Activity can be a Movement (copying of data from source to destination), Transformation ( a Databricks notebook or stored procedure), if-else conditions for dynamic pipeline control

2) Datasets: A named reference to a specific piece of data: a table, a file or a folder with metadata such as its format and schema. It holds no data itself.

3) Data Flow: No-code, visual transformations like filters, aggregates, joins, etc. Power Query is the same except it's a Power BI style query editor style (for Data Analysts)

4) Change Data Capture: Unlike a pipeline, a resource that runs continuously. It tracks changes to data given by the source database such as inserts, updates, deletes and applies them to the destination.

- Step 9: Batch Export Data from PostHog to Data Lakehouse

Backfilling all historical data (1st April, 2026 - 7th October, 2026) from PostHog in the form of Parquet files partitoned via 
date (`{table}/{year}/{month}/{day}/`) into the Storage account with hierarchical namespace enabled (makes it a data lake).

PostHog connects to our Storage account via its Connection string and exports all the data into the Container `landing`, i.e, the Data LAKE layer since PostHog exports in the form of plain Parquet files, not Delta files. The Bronze layer is where the Data LAKEHOUSE architecture begins.

## PostgreSQL (Supabase)

As mentioned earlier, the Events that are streamed into Event Hub represent factual data. To give it more context, using ADF to incremental batch ingest dimensional data (users, workouts, and gyms) from the PostgreSQL database into the Lakehouse.

- Step 10: Connect Supabase (PostgreSQL) Database to ADF

- Step 10.1: Create a new `adf_reader` role in the Server
- Step 10.2: Grant CONNECT on 'postgres' Database, USAGE on 'public' schema, and SELECT on 'public.gyms' & 'public.workouts' (Saved in SQL Editor)

```sql
CREATE ROLE adf_reader WITH LOGIN PASSWORD 'password';

GRANT CONNECT ON DATABASE postgres TO adf_reader;
GRANT USAGE ON SCHEMA public TO adf_reader;
GRANT SELECT ON public.workouts, public.gyms TO adf_reader;
```

- Step 10.3: Create a View on 'public.users' to mask PII and give adf_reader access to the View instead of the table directly. The View is created in a new `analytics` schema to ensure seperation of concerns between app-related objects and pipeline-related objects.

```sql
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE VIEW analytics.users_masked AS
SELECT
  id,
  encode(sha256(lower(email)::bytea), 'hex')  AS email_hash,
  split_part(email, '@', 2)                   AS email_domain,
  left(nullif(first_name, ''), 1) || '***'    AS first_name_masked,
  left(nullif(last_name, ''), 1)  || '***'    AS last_name_masked,
  left(nullif(username, ''), 2)   || '***'    AS username_masked,
  created_at,
  updated_at
FROM public.users;

-- Granting adf_reader access to Masked View instead of direct table public.users
GRANT USAGE ON SCHEMA analytics TO adf_reader;
GRANT SELECT ON analytics.users_masked TO adf_reader;
```

- Step 10.4: Create RLS Policies

GRANT and RLS policies are two separate checks that answer different questions, and a query has to pass both.
`GRANT SELECT` works at the table level. It decides whether the role may run a SELECT against the table at all.
An RLS policy works at the row level. It decides which rows that role can see once it's allowed in.

When RLS is enabled on a table, Postgres applies a default-deny rule: a role sees only the rows that some policy allows for it. If no policy applies to the role, the set of visible rows is empty.

```sql
CREATE POLICY "adf_reader can read workouts"
ON public.workouts
FOR SELECT
TO adf_reader -- role to apply policy to
USING (true); -- predicate condition: rows that return TRUE are displayed

CREATE POLICY "adf_reader can read gyms"
ON public.gyms
FOR SELECT
TO adf_reader
USING (true);
```

- Step 10.5: Create Linked Service in ADF to PostgreSQL

Using the 'Session Pooler' Connection string in Supabase to expose an end-point that ADF can connect to. 'Session Pooler' is an alternative to direct connection (which uses IPv6) when connecting via an IPv4 network. ADF defaults to IPv4 network connections.

Created Linked Service in ADF uses `Host`, `Port`, `Database`, `User name`, and `Password` fields that are provided by the Session Pooler connection string.

However, TLS (formerly SSL) provides two layers of security on the connection:
1) Encryption: Ensures data in transit is encrypted
2) Identity verification: Authentication check by the client on the Certificate provided by the server to ensure we're talking to the real server

We're setting SSL Mode to 'Require' in the Linked Service. This implies that TLS must only guarantee Encryption, not Identity Verification. 
Hence, an additional key-value field must be passed in the Linked Service explicitly stating that we trust whatever certificate the server returns without needing to verify it (`TrustServerCertificate = true`).

## Datasets

How do I connect to a system? Linked Service - Host, port, database, user name, password
Which data in that system? Dataset - Schema and table
What to do with the data? Activity - Copy to the lake

A dataset is a reference to the data sitting in one of the external systems that we're connected to via a Linked Service. So, every dataset refers to exactly one Linked Service. Additionally, it also stores metadata to interpret the data such as its format and schema.

One linked service supports many datasets, and one dataset can be used by many activities.

- Step 11: Create Source Dataset (PostgreSQL)

Create Dataset via the PostgreSQL Linked Service and define Schema and Table as parameters

- Step 12: Create Sink Dataset (Data Lake)

Create Dataset via the Data Lake Linked Service to connect to the Storage Account. Select 'landing' as the Container and 
`@concat('dimensions/', dataset().folder)` as the Directory in order to parameterize the created directories (not dataset().table since our View is named 'users_masked' instead of 'users').

The 'landing' container is part of the Data Lake since that's where all external systems write to in the form of plain parquet files. The bronze, silver, and gold layers are what the SDP will operate on and produce Delta files within - hence, making that a Delta Lake.

### Parameterized Datasets

A dataset pointing at one fixed table means ten tables need ten source datasets and ten sink datasets.
We define parameters on the dataset such as `@dataset().table`. The activity then supplies its value during runtime.

Hence, one source dataset defined for all the schemas and tables. One sink dataset referring to different folders in the Data Lake.

**Pipeline parameter**: Referred in activities via `@pipeline().parameters.x`
Input passed at the start of a pipeline run that makes it dynamic. Available to every activity in the pipeline, but never passed automatically.

**Dataset parameter**: Referred in datasets via `@dataset().x`
Input passed into a dataset everytime it is used by an activity. Makes it dynamic by enabling reference to more than one schema and table.

Value Chain in Dimension Load Pipeline: pipeline parameter `tables` → ForEach Items → `@item()` → dataset parameters
- ForEach reads `@pipeline().parameters.tables` as the 'Items' iterable it requires.
- Every forEach iteration exposes each entry as `@item()`
- Copy reads `@item().schema`, `@item().table`, `@item().folder` and passes them to the datasets.
- Datasets only see their own parameters via `@dataset().x`

## ForEach Activity

A control activity in ADF Pipelines that runs the same set of inner activities once for each element of an ARRAY. It's the pipeline equivalent of a for loop.

**Items**: The array to loop over. It can come from a pipeline parameter, such as `@pipeline().parameters.tableNames`, or from an earlier activity's output, such as a Lookup or Get Metadata.

This allows Copy activity looping over users, workouts and gyms, instead of three near-identical pipelines.

**Copy Activity**: ADF's core data migration tool. It reads from a source, optionally converts the format, and writes to a sink (destination).

**Trigger**: ADF Pipeline Trigger set for a daily schedule which overwrites the same 3 files with the current state of the source. No performance issues on small dimensions tables such as in our case.

## Spark Declarative Pipeline

Implementing the SDP that ingests data from the data lake layer 'landing' and performs ETL transformations

SOURCES                 BRONZE                       SILVER                    GOLD

Event Hubs ───────────> bronze_events_stream ───┐
(live events)           [streaming table]       ├──> silver_events ──────────> Fact tables
                                                │    [streaming table]         [materialized views]
Landing: backfill ────> bronze_events_backfill ─┘    unified, deduplicated       ^
(event files)           [streaming table]                                        │
                                                                                 │
Landing: dimensions ──> bronze_users/workouts/gyms ─> silver_users/workouts/gyms ┤
(overwritten files)     [materialized views]          [materialized views]       │
                                                      typed, no PII              └──> Dimension tables
                                                                                      [materialized views]

- Step 1: Create Bronze, Silver, and Gold layers in the ADLS Gen2 Storage Account as distinct containers

- Step 2: Create Access Connector and grant 'Storage Blob Data Contributor' on the Storage Account

- Step 3: Create a Storage Credential to the Storage Account and three External Locations to the three Containers

- Step 4: Create a Catalog 'apex' and 'bronze', 'silver, and 'gold' Schemas with Managed Locations as the three containers

### bronze_dimensions.py

Creating the ETL pipeline first, then writing its logic as plain Python or SQL files inside the pipeline’s project folder. This contrasts with the older approach wherein we first wrote the SDP code independently in notebooks and attached them to the pipeline afterwards. 

Both have the same performance. The ETL-Pipeline-First approach is preferred since it allows easier development.

Pipeline Project Folder Structure:
`transformations/`: the pipeline’s source code. Every file here is included automatically, so a new file needs no settings change.
`explorations/`: notebooks for interactive checks. They sit beside the project but are not run by the pipeline.
`utilities/`: shared Python code, such as schemas or helper functions, imported like a normal module.

Dry run: validates the code and builds the graph without processing data. Use it to catch errors cheaply.
Run pipeline: processes only new data for streaming tables and brings materialized views up to date.
Full refresh: discards a table’s data and state and rebuilds from the source. Be careful with this on the Event Hubs table, because the hub only retains events for a limited time, so anything older would be lost for good.

- Step 5: Creating `bronze_dimensions.py`

Creating the script that ingests the three dimensions table from the Data Lake (landing) into the Bronze layer

```python
from pyspark import pipelines as dp
from pyspark.sql import functions
from pyspark.sql.functions import col

landing = 'abfss://landing@<dl***>.dfs.core.windows.net/dimensions'
def all_bronze_dimensions(table_name):
    @dp.materialized_view(
        name = f"apex.bronze.{table_name}",
        comment = f"Bronze table for {table_name} that ingests raw data from Landing into Delta Lake"
    )

    def bronze_dimension():
        return (
            spark.read.format('parquet').load(f"{landing}/{table_name}") \
            .withColumn('file_path', col('_metadata.file_path')) \
            .withColumn('file_modification_time', col('_metadata.file_modification_time'))
        )
for table_name in ['users', 'gyms', 'workouts']:
    all_bronze_dimensions(table_name)
```

### bronze_batch_events.py

Identifying schema of Batch Events in the landing layer to ensure PII is removed before loading into the Bronze layer

```python
path = "abfss://landing@dlstreamprojdev.dfs.core.windows.net/events/"
df = spark.read.option("recursiveFileLookup", "true").parquet(path) # DF Reader API only looks into files directly in the load folder by default
df.printSchema()
display(df.limit(5))
print(df.count())
```

Dropping the entire 'person_properties' column and some keys in the 'properties' column to filter out PII.

#### Struct vs Map

Two ways to describe a JSON Object.

Struct asks for the name and type of each field - any fields not mentioned are dropped. It allows different types for each field.
`struct<$session_id:string, $screen_width:int, method:string, ...>`

Map asks for one type for all keys and one type for all values. The keys in the JSON don't have to be listed. Value types for all keys are the same
`map<string, string>` 

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

landing = 'abfss://landing@dlstreamprojdev.dfs.core.windows.net/events'

SENSITIVE_KEYS = [
    "$ip",
    "$geoip_latitude",
    "$geoip_longitude",
    "$geoip_postal_code",
    "$set",
    "$set_once",
]

@dp.table(
    name='apex.bronze.batch_events',
    comment='Ingesting raw events for backfilled data that also runs daily for present data with PII removed'
)

def batch_and_backfill_events():
    return (
        spark.readStream.format('cloudFiles') \
            .option('cloudFiles.format', 'parquet') \
            .option('pathGlobFilter', '*.parquet') # ensures only parquet files are ingested
            .load(landing) \
            .drop('person_properties') \
            .withColumn('properties', F.map_filter(F.from_json('properties', 'map<string, string>'),
                                                   lambda key,val: ~key.isin(SENSITIVE_KEYS)))
            .withColumn('_ingested_at', F.current_timestamp())
            .withColumn('_source_file', F.col('_metadata.file_path'))
    )
```

`from_json()` parses the JSON String 'properties' column into a JSON Map. Thus, each key-value pair is now accessible, enabling us to remove keys that include Sensitive PII.

***Note***: Unlike the DataFrame Reader API, Auto Loader looks up the load path recursively by default. So, pointing it at the 'events' folder picks up every file in every date subfolder, however deep.

### bronze_stream_events.py

First, creating a Secret in the workspace to store the EventHub's listenPolicy connection string that's needed to authenticate Databricks to listen to the EventHub instance.

All workspace-local resource manipulations via the Databricks UI (creating jobs, managing clusters, storing secrets) can also be done in Python code via the `WorkspaceClient` API

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.secrets.create_scope(scope='apex')
w.secrets.put_secret(
    scope='apex',
    key='eventhubs-listen-connection',
    string_value='Endpoint=sb://***'
)
```
***Note***: `scope` is a named container for secrets 

Creating `transformations/bronze_stream_events.py`

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

EH_namespace = 'streaming-project-events'
EH_instance = 'streaming-project-topic'

connection_string = dbutils.secrets.get(scope='apex', key='eventhubs-listen-connection')

@dp.table(
    name='apex.bronze.stream_events',
    comment='Ingesting raw events from Event Hub into the Bronze layer'
)

def ingest_stream_events():
    return (
        spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', f'{EH_namespace}.servicebus.windows.net:9093')
        .option('subscribe', f'{EH_instance}')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='$ConnectionString' password='{connection_string}';")
        .option('startingOffsets', 'earliest')
        .load()
        .select(F.col('value').cast('string').alias('body'), F.col('partition'), F.col('offset'), F.col('timestamp').alias('enqueued_at'), F.current_timestamp().alias('_ingested_at'))
    )
```

To connect with EventHub, it is treated like Kafka - the namespace is the Kafka server, using port 9093, and the instance is the topic.

`kafka.security.protocol: SASL_SSL`: Sets how the connection is secured. `SSL` means all traffic is encrypted in transit (TLS). `SASL` means the client must authenticate before it can do anything. Both are required by EventHub to allow a connection from the client.

`kafka.sasl.mechanism: PLAIN`: Implies that we're using a simple username and password authentication

`kafka.sasl.jaas.config`: Supplies the actual credentials for login
`kafkashaded...PlainLoginModule`: Databricks-specific config
`username="$ConnectionString`: A fixed, literal value that tells EventHub that the password is a connection string

`value` arrives as bytes - casting it to a string yields the JSON body
`partition` and `offset` uniquely identify each message.
`failOnDataLoss: false` is set to true by default that fails the pipeline in cases wherein the checkpoint expects to read offset 500 but the earliest avaiable one at the time of the pipeline run is 800 - this implies data was lost between subsequent runs so default behaviour is to fail

### silver_events.py

`apex.silver.events` combines both event sources, gives columns proper types, removes out-of-scope events and keeps one row per event (uuid)

`apex.bronze.stream_events`: One JSON String per event in `body` + partition, offset, enqueued_at, _ingested_at
`apex.bronze.batch_events`: Core columns (uuid, event, timestamp) + `properties` column of type JSON Object/Map `map<string, string>`

Stream -> JSON String with plain names (city, session_id) in the `body` column, because the webhook template renamed them.
Batch -> PostHog’s original names inside the JSON Map ($geoip_city_name, $session_id), all values as text ("402", "true").

- The 4 Datasets
`stream_conformed`: Temporary View. Used to parse the JSON String and pivot each key into a column
`batch_conformed`: Temporary View. Used to pull out same columns from the JSON Map and give them the same name 
`events_unioned`: Temporary View. Used to combine the two via `unionByName`, type the columns, and performs data quality checks
`apex.silver.events`: Streaming Table. Used to dedup on `uuid` using AUTO CDC

How are we merging two sources having different schema? Conform both to one schema, convert all columns to StringType(), union them, then cast back

#### PROPERTY FIELDS and CORE FIELDS

First, creating a dictionary `PROPERTY_FIELDS` for Batch Events that consists of keys as the expected silver table column headers and values as their corresponding fields in the 'properties' column of Batch Events. 

Note that Stream events already has its fields renamed to the keys of this dictionary.

```python
PROPERTY_FIELDS = {
    "session_id": "$session_id",
    "is_identified": "$is_identified",
    "app_version": "$app_version",
    "app_build": "$app_build",
    "device_name": "$device_name",
    "device_type": "$device_type",
    "os_name": "$os_name",
    "os_version": "$os_version",
    "locale": "$locale",
    "timezone": "$timezone",
    "city": "$geoip_city_name",
    "region": "$geoip_subdivision_1_name",
    "country_code": "$geoip_country_code",
    "workout_name": "workout_name",
    "gym_slot": "gym_slot",
    "is_change": "is_change",
    "reason": "reason",
    "method": "method",
    "missing_email": "missing_email",
    "permission_type": "type",
    "new_goal": "new_goal",
    "new_tier": "new_tier",
    "nudge_type": "nudgeType",
    "result": "result",
    "tour_id": "tour_id",
    "total_steps": "total_steps",
    "steps_viewed": "steps_viewed",
    "release_key": "release_key",
    "total_features": "total_features",
    "feature_id": "feature_id",
    "step_number": "step_number",
    "elapsed_ms": "elapsedMs",
    "cached_token_present": "cachedTokenPresent",
}
```

To define the SCHEMA of the Stream Events data, we're using the keys in `PROPERTY_FIELDS` as well as a `CORE_FIELDS` list which is essentially all the remaining fields in the Stream Events outside of the fields listed in `PROPERTY_FIELDS`

```python
CORE_FIELDS = ["uuid", "event", "timestamp", "captured_at", "distinct_id", "person_id"]
```

Now, defining the STREAM_SCHEMA which will be used to convert the JSON String `body` to JSON Struct with all fields of type StringType()
The `CORE_FIELDS` will first be explicitly converted back to their original types since Batch Events already have those in their original types too

```python
STREAM_SCHEMA = StructType([StructField(name, StringType()) for name in CORE_FIELDS + list(PROPERTY_FIELDS)])

@dp.temporary_view()
def stream_conformed():
    parsed = spark.readStream.table('apex.bronze.stream_events') \
        .withColumn('e', F.from_json('body', STREAM_SCHEMA))
    return (
        parsed.select(
            F.col('e.uuid').alias('uuid'), # explicit casting of CORE_FIELDS first
            F.col('e.event').alias('event'),
            F.col('e.timestamp').cast(Timestamp()).alias('event_time'),
            F.col('e.captured_at').cast(Timestamp()).alias('captured_at'),
            F.col('e.distinct_id').alias('distinct_id'),
            F.col('e.person_id').alias('person_id'),
            *[F.col(f'e.{name}').alias(f'{name}') for name in PROPERTY_FIELD], # *[] unpacks all elements of the list of column expressions
            F.lit('stream').alias('source'),
            F.col('_ingested_at')
        )
    )
```

`list(PROPERTY_FIELDS)`: Storing a dictionary as a list stores all its keys only
`PROPERTY_FIELD`: Iterating over a dictionary directly only gives access to its keys
`PROPERTY_FIELDS.items()`: Iterating with .items() over the dictionary gives access to its key-value pairs

Now, the CORE_FIELDS already exist in the Batch Events as columns, so keeping them as it is. Then, extracting PROPERTY_FIELDS from the 'property' map with renamed column headers

```python
@dp.temporary_view()
def batch_events():
    return (
        spark.readStream.table('apex.bronze.batch_events').select(
            F.col('uuid'),
            F.col('event'),
            F.col('timestamp').alias('event_time'),
            F.lit(None).cast(Timestamp()).alias('captured_at'), # lit(None) == NULL - thus, can be converted to any data type
            F.col('distinct_id'),
            F.col('person_id'),
            *[F.col(f'{val}').alias(f'{key}') for key, val in PROPERTY_FIELDS.items()]
            F.lit('batch').alias('source')
            F.col('_ingested_at')
        )
    )
```

Both `stream_conformed` and `batch_conformed` have the same schema and hence, are union-compatible
Delineating the columns that must be typed to their correct types AFTER the union and removing the ones not needed in the silver table :

```python
INT_COLUMNS = [
    "elapsed_ms",
    "new_goal",
    "step_number",
    "total_steps",
    "steps_viewed",
    "total_features",
]
BOOL_COLUMNS = ["is_identified", "is_change", "missing_email", "cached_token_present"]

# Out of scope columns that aren't needed (system and auth-plumbing events)
EXCLUDED_EVENTS = [
    "$set",
    "$identify",
    "signin_stale_token_found",
    "signin_stale_token_deleted",
    "supabase_token_retry",
]
```

Now, creating the unioned dataset that performs data quality checks, combines the two sources, and types them correctly

***Note***:`stream_conformed` is the name of a Python function, not a DataFrame. 
Hence, the DataFrame/DataStream Reader API must be used whenever referring to a Dataset anywhere again.

Temp View is a source to a Streaming Table -> DataStream Reader API
Temp View is a source to a Materialized View -> DataFrame Reader API

```python
@dp.temporary_view()
@dp.expect_all_or_drop({
    'uuid_present': 'uuid IS NOT NULL',
    'event_time_present': 'event_time IS NOT NULL'
})
@dp.expect('event_time_not_in_future', 'event_time <= current_time() + INTERVAL 1 DAY') # one-day buffer to avoid warning from trivial clock skews
def events_unioned():
    unioned = spark.readStream.table('stream_conformed').unionByName(spark.readStream.table('batch_conformed')) # DS Reader API to refer datasets
    for name in INT_COLUMNS:
        unioned = unioned.withColumn(name, F.col(name).try_cast('bigint'))
    for name in BOOL_COLUMNS:
        unioned = unioned.withColumn(name, F.col(name).try_cast('boolean'))
    return (
        unioned.filter(~F.col('event').isin(EXCLUDED_EVENTS)) \
            .withColumn('_source_priority', F.when(col('source') = 'batch', 1).otherwise(0))
    )
```

`_source_priority`: When an event exists in both sources, the two copies sometimes disagree on event time by milliseconds upto about minutes. The pipeline has to pick one, and it picks batch since it's PostHog's final version of the event.

Hence, the stream source allows near real-time ingestion of events while the batch source is delayed but the final version fo the event in our pipeline.

Finally, upserting into the Silver table using Auto CDC
We upsert even though events inherently are append-only because we have two sources of truths for events into the pipeline which may append the same event in different runs of the pipeline

```python
dp.create_streaming_table(
    name='apex.silver.events',
    comment='All PostHog events from the stream and the batch bronze tables, typed, scoped and deduplicated on uuid. When an event arrives from both sources, the batch copy is kept."
)

dp.create_auto_cdc_flow(
    target='apex.silver.events',
    source='events_unioned', # As AUTO CDC requires source input, this is why DataStream Reader API was used to define it
    keys=['uuid'],
    sequence_by=F.struct('_source_priority', '_ingested_at'),
    except_column_list=['_source_priority'],
    stored_as_scd_type=1
)
```

`sequence_by` accepts only one column. The documented way to sequence by several columns is to wrap them in a struct and then pass them.
A struct is still one column, but Spark compares structs field by field: compare `_source_priority` first, `_ingested_at` as a tie-breaker