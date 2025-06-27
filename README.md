# Omni Channel to BigQuery App

This application provides a simple GUI to fetch data from an Omni Channel platform (currently Caresoft), normalize and sort it, and then load it into Google BigQuery. A future enhancement will introduce a Kafka publishing step to enforce schema validation before ingestion.

## Table of Contents

* [Features](#features)
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [Configuration](#configuration)
* [Usage](#usage)
* [Pipeline Flow](#pipeline-flow)
* [Future Improvements](#future-improvements)
* [License](#license)

---

## Features

* Select a date/time range via a Tkinter GUI
* Fetch paginated JSON data from the Omni Channel (Caresoft) API
* Automatic retry logic on API failures
* Normalize column types (Integer, Timestamp, String) using pandas
* Sort records by `created_at`
* Preview the first 5 rows in the console
* List, import, and merge tables in BigQuery
* Interactive prompts for creating new tables or updating existing ones

## Prerequisites

* Python 3.8+
* Access to the Omni Channel (Caresoft) API
* Google Cloud service account with BigQuery permissions
* (Future) Kafka cluster & Schema Registry for schema enforcement

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/kina2711/datapipeline_omnichanneltobigquery.git
   cd datapipeline_omnichanneltobigquery
   ```
2. Create a virtual environment and install dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate    # Linux/macOS
   venv\\Scripts\\activate     # Windows
   pip install -r requirements.txt
   ```

## Configuration

1. Copy the sample environment file and edit credentials:

   ```bash
   cp information.env.sample information.env
   ```
2. Set the following variables in `information.env`:

   ```dotenv
   API_KEY=<YOUR_OMNI_CHANNEL_API_KEY>    # Caresoft API key
   GCP_KEYFILE=path/to/your-keyfile.json
   GCP_PROJECT=<YOUR_PROJECT_ID>
   BQ_DATASET=<YOUR_DATASET_NAME>
   # (Future) Kafka settings
   KAFKA_BOOTSTRAP=<bootstrap.servers>
   SCHEMA_REGISTRY_URL=<schema.registry.url>
   ```

## Usage

Run the main application:

```bash
python main.py
```

1. In the GUI, select the **start** and **end** date/time.
2. Click **"Lấy API & Đẩy BigQuery"**.
3. Wait for data fetching and processing logs in the console.
4. Choose **new** to create/import into a new BigQuery table, or **update** to merge into an existing table.

---

## Pipeline Flow

```mermaid
flowchart LR
    A[Start GUI] --> B[Select Date & Time]
    B --> C[Fetch Data from Omni Channel (Caresoft) API]
    C --> D[Normalize & Cast Columns]
    D --> E[Sort by created_at]
    E --> F[Show Preview]
    F --> G{Action?}
    G -->|New| H[Import to BigQuery as New Table]
    G -->|Update| I[Import to Staging Table]
    I --> J[Merge Staging into Main Table]
    J --> K[Drop Staging Table]
    
    %% Future: Kafka Integration Step
    D -.-> KAFKA[Publish to Kafka for Schema Enforcement]
    KAFKA -.-> E
```

1. **Fetch**: Retrieves JSON records in pages with retry logic from Caresoft.
2. **Normalize**: Casts columns to appropriate types using pandas.
3. **Sort**: Orders records by timestamp.
4. **Preview**: Prints head of DataFrame.
5. **Load**: Imports or merges into BigQuery.
6. **Cleanup**: Removes temporary staging tables.
7. **(Future)** Kafka step between Normalization and Sort to enforce schema via Avro/JSON Schema.

## Future Improvements

* **Kafka Integration**: Publish each record to a Kafka topic using a predefined Avro schema, validating data before BigQuery ingestion.
* **Asynchronous Publishing**: Batch and asynchronously publish to Kafka for better performance.
* **CLI Support**: Offer command-line parameters in addition to GUI.
* **Dockerization**: Containerize the app for easy deployment.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
