# New York Taxi ETL Pipeline

An automated data pipeline that extracts New York taxi trip data from a ClickHouse database, transforms it into a monthly weekend summary, and loads it into a local SQLite database. The pipeline runs on a schedule using Apache Airflow, containerised with Docker.

---

## What This Project Does

Every month, this pipeline automatically:

1. **Connects** to a ClickHouse database containing New York taxi trip records
2. **Queries** the data to calculate Saturday and Sunday averages per month (trip count, fare amount, and trip duration) for the years 2014–2016
3. **Saves** the results into a local SQLite database called `newyorktaxi.db`

The pipeline runs at midnight on the first day of every month.

---

## Project Structure

```
airflow_etl/
├── dags/
│   ├── newyorktrip_etl_dag.py  # Tells Airflow when and how to run the pipeline
│   └── newyorktrip_etl.py      # The actual ETL logic (connect, query, save)
├── logs/                       # Airflow stores run logs here automatically
├── .env                        # Environment variables for Docker
├── docker-compose.yaml         # Sets up all the services needed to run Airflow
└── README.md
```

---

## Prerequisites

Before you can run this project, you need the following installed on your machine:

- **Docker Desktop** — [Download here](https://www.docker.com/products/docker-desktop). This runs the whole project inside containers so you don't have to install everything manually.
- **A running ClickHouse database** with a table called `tripdata` containing New York taxi data, including the columns: `pickup_datetime`, `dropoff_datetime`, and `fare_amount`.

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/bukolaakins/airflow_etl.git
cd airflow_etl
```

### 2. Create your ClickHouse connection file

Inside the `config/` folder, create a file called `conn_string.json`. This file tells the pipeline how to connect to your ClickHouse database.

```json
{
  "host": "your-clickhouse-host",
  "port": 8123,
  "username": "your-username",
  "password": "your-password"
}
```

Replace the placeholder values with your actual ClickHouse connection details.

> ⚠️ **Important:** This file contains sensitive credentials. It is listed in `.gitignore` and should never be committed to version control.

### 3. Set your Airflow user ID (Linux/Mac only)

Run this command in your terminal from the project root. It creates a `.env` file that Docker needs:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

On Windows, you can skip this step or manually create a `.env` file with:

```
AIRFLOW_UID=50000
```

### 4. Initialise Airflow

This only needs to be run once, the very first time you set up the project:

```bash
docker compose up airflow-init
```

### 5. Start all services

```bash
docker compose up -d
```

The `-d` flag runs everything in the background. This starts:

- **PostgreSQL** — Airflow's internal database
- **Redis** — A message broker Airflow uses to manage task queues
- **Airflow Webserver** — The visual dashboard (available at `http://localhost:8080`)
- **Airflow Scheduler** — Watches for DAGs that are due to run
- **Airflow Worker** — Actually executes the tasks

### 6. Access the Airflow UI

Open your browser and go to: [http://localhost:8080](http://localhost:8080)

Default login credentials:
- **Username:** `airflow`
- **Password:** `airflow`

### 7. Enable the DAG

In the Airflow UI, find the DAG called `newyorktrip_etl_dag` and toggle it on. It will run automatically at midnight on the first of each month, or you can trigger it manually by clicking the play button.

---

## How the Pipeline Works

### `newyorktrip_etl.py` — The ETL Logic

This file contains three functions:

| Function | What it does |
|---|---|
| `connect_to_clickhousedb()` | Opens a connection to ClickHouse using the credentials in `conn_string.json` |
| `extract_db_metrics(client)` | Runs a SQL query to get monthly Saturday/Sunday averages and returns a DataFrame |
| `load_to_db(df, db_path)` | Saves the DataFrame into a SQLite table called `monthlymetrics` |

### `newyorktrip_etl_dag.py` — The Airflow Schedule

This file defines the **DAG** (Directed Acyclic Graph) — Airflow's word for a scheduled workflow. It:

- Sets the schedule to run at midnight on the 1st of every month (`0 0 1 * *`)
- Calls the three ETL functions in order
- Raises an error and stops if the database connection or data query fails
- Saves the output to `/opt/airflow/newyorktaxi.db` inside the container

### The SQL Query

The query uses a two-step approach:

1. **Step 1 (CTE):** Groups all trips by day of week and month, calculating average fare, trip count, and average duration
2. **Step 2 (outer query):** Pivots that data so each row is one month, with separate columns for Saturday and Sunday metrics

The final output has these columns:

| Column | Description |
|---|---|
| `month` | Month and year (e.g. `01-2014`) |
| `sat_mean_trip_count` | Average number of Saturday trips that month |
| `sat_mean_fare_per_trip` | Average Saturday fare |
| `sat_mean_duration_per_trip` | Average Saturday trip duration (seconds) |
| `sun_mean_trip_count` | Average number of Sunday trips that month |
| `sun_mean_fare_per_trip` | Average Sunday fare |
| `sun_mean_duration_per_trip` | Average Sunday trip duration (seconds) |

---

## Stopping the Project

To stop all running services:

```bash
docker compose down
```

To stop and also delete the stored data (the Postgres database volume):

```bash
docker compose down --volumes
```

---

## Dependencies

The `clickhouse-connect` Python package is installed automatically when Docker starts, as specified in `docker-compose.yaml`. The other dependencies (`pandas`, `sqlite3`) are part of the standard Airflow Docker image.

---

## Troubleshooting

**DAG not appearing in the UI?**
Make sure both `.py` files are inside the `dags/` folder and that the Airflow scheduler has had a moment to pick them up (usually within 30 seconds).

**Connection to ClickHouse failing?**
Double-check the values in `config/conn_string.json`. Make sure your ClickHouse instance is reachable from inside the Docker network.

**Not enough memory warning on startup?**
Docker requires at least 4GB of RAM. Check your Docker Desktop memory settings under Preferences → Resources.
