# Trino OpenSource Data Lakehouse

This project demonstrates how to build a cost-effective and efficient lakehouse using open-source technology.

We will take data from a typical transactional database, load it into a data lake, enhance the data lake by adding a catalog on top of it, and convert it into a lakehouse.

### Components of This Project

1. **Amazon S3**:
   - Acts as the primary storage layer.
   - Stores raw data, as well as processed data.

2. **Apache Iceberg**:
   - Manages large sets of tabular data on S3.
   - Provides features like ACID transactions, schema evolution, and partitioning.

3. **Project Nessie**:
   - Acts as a catalog for managing Iceberg tables.
   - Provides a Git-like experience for data, allowing for version control and branching.
   - Manages the Iceberg metadata and schema versions.

4. **Trino (formerly PrestoSQL)**:
   - A SQL query engine that interacts with Iceberg tables.
   - Enables data analytics and querying across the data stored in S3.

### Data Flow

- **Data Ingestion**: Data is ingested into Amazon S3, where raw data is stored.
- **Iceberg Management**: Apache Iceberg manages the metadata and schema of the data stored in S3, ensuring efficient data handling and queries.
- **Catalog and Version Control**: Project Nessie acts as the catalog for Iceberg tables and provides version control, allowing for safe and manageable data changes.
- **Data Querying**: Trino acts as the SQL query engine, enabling users to perform analytics and process data efficiently.

### Prerequisites

- Docker installed on your machine. Preferably, Docker Desktop.

### Step-by-Step Guide

#### Step 1: Clone the Repository

```bash
git clone <repository_url>
cd <repository_name>
```

#### Step 2: create a python virtual env

```
python -m env <environment_name>
```
- Or use below if you prefer conda (anaconda)

```
conda create -n <environment_name> 
```


#### Step 3: Activate the python virtual env

```
source <environment_name>/bin/activate
```
- Or use below if you prefer conda (anaconda)

```
conda activate <environment_name>
```

#### Step 4: install dependencies and fill configs

```bash
python install -r requirements.txt
```
```bash
cd into scripts, locate config.py and fill it up as necessary
```
```bash
cd into trino-set, locate catalog.properties and fill it up as necessary
```



#### Step 5: Start start project nessie

```bash
docker compose up catalog
```


#### step 6: Move data from source into S3 Bronze bucket

```bash
python incremental_load_nessie.py
```

#### step 7: Start Trino

```bash
docker compose up trino
```


#### Step 8:  Start Kafdrop to see the sync data.

```bash
goto localhost:8080 and create a user
```

#### Step 9:  Connect to Trino on terminal

```bash
docker compose exec -it trino trino
```
 
#### Step 10:  check existing catalogs

```bash
SHOW CATALOGS;
```

#### Step 11:  check existing Schema in a particular catalog

```bash
SHOW SCHEMAS IN < CATALOG_NAME >;
```

#### Step 12:  check existing tables in a particular schema

```bash
SHOW TABLES IN < CATALOG_NAME >.<SCHEMA_NAME >;
```

#### Step 13:  query a tables in a particular schema

```bash
SELECT * FROM < CATALOG_NAME >.<SCHEMA_NAME >.< TABLE_NAME >;
```

#### Step 13:  Create a Schema and bind it to a directory in S3

```bash
CREATE SCHEMA silver
WITH (
    location = 's3://<S3_BUCKET_NAME>/silver/'
);
```




##### Extras

> You can connect to trino using DBeaver and other tools to enable you run your query in a GUI.
> you can also connect to Tableau


