# MTA_ridership

This project contains three data pipelines that are scheduled and managed using Apache Airflow. The pipelines are designed to extract, transform, and load data related to the Metropolitan Transportation Authority (MTA) ridership.

## Getting Started

To get started with this project, you need to clone this repository to your local machine, install the dependencies, and run the scripts. Here are the steps:

### Clone the repository

Use git to clone this repository to your local machine:

```bash
git clone https://github.com/timothyyang96/MTA_ridership.git
```

Then, change your current directory to the project directory:

```bash
cd MTA_ridership
```

### Install the dependencies

The project requires Python 3.x and the following libraries:

- pandas
- sqlalchemy
- apache-airflow

You can install these libraries using pip:

```bash
pip install -r requirements.txt
```

This command will install all the dependencies listed in the requirements.txt file in your project.

### Start the Airflow server

Run the Airflow webserver and scheduler:

```bash
airflow webserver -D
airflow scheduler -D
```

This will start the Airflow webserver and scheduler on your local machine.

## Data Pipelines

The project contains three data pipelines:

1. `mta_data_pipeline_hourly_ridership`: This pipeline extracts hourly ridership data from the MTA's API, processes the data, and stores it in a MySQL database.
2. `mta_data_pipeline_wifi`: This pipeline extracts WiFi location data from the MTA's API, processes the data, and stores it in a MySQL database.
3. `mta_data_pipeline_third`: This pipeline waits for the first two pipelines to complete, then combines the ridership and WiFi data into a single table in the MySQL database.

## Running the pipelines

To run the pipelines, you need to trigger them from the Airflow webserver. Here are the steps:

1. Open the Airflow webserver in your web browser. By default, the webserver runs at `http://localhost:8080`.
2. Click on the DAGs menu item in the top navigation bar. This will display a list of all the available DAGs.
3. Find the DAGs for the three pipelines (`mta_data_pipeline_hourly_ridership`, `mta_data_pipeline_wifi`, `mta_data_pipeline_third`) and click on the play button next to each DAG to trigger it.
4. You can monitor the progress of the pipelines from the Airflow webserver.
