# BEES Data Engineering Case

## Table of Contents

- [About](#about)
- [Getting Started](#getting_started)
- [Usage](#usage)
- [Contributing](../CONTRIBUTING.md)

## About <a name = "about"></a>

The purpose of this project is to show a ETL processes data from an API and transform it at a analytical purpose, considering the following tools:
- Airflow
- AWS S3
- AWS Glue (PySpark)


### Getting Started

This project uses an Airflow solution that triggers tasks to fetch data from a public API.
It is considered to use Airflow as a orchestrator with AWS GLue transformations tasks within, to guarantee the Medalion Archtecture to be processed and stored into a S3 bucket (cloud storage).

### Prerequisites

You have to install Docker before the execution of the Airflow. After that, execute the following code to initiate the Airflow

### Installing

The instructions below cover Linux, macOS, and Windows.
For Linux and macOS

1. Download Docker Compose:
```
sudo curl -L "https://github.com/docker/compose/releases/download/v2.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
```
2. Aplly Execute permitions.
```
sudo chmod +x /usr/local/bin/docker-compose
```
3. Check if it is installed
```
docker-compose --version
```

## Usage <a name = "usage"></a>

For usage, you need to start docker locally.
```
docker-compose up airflow-init
docker-compose up -d
```

After that, access the Airflow: 
- Go to http://localhost:8080 in your browser. 
- Use the credentials you set (admin/admin if you used the example).

And to shut it down, you need this code.
```
docker-compose down
```
