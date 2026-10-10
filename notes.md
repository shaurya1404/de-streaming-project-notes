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

- Step 4: Create a Catalog 'apex' and three Schemas with Managed Locations as the three containers
