# FARMWISE API

## Table of Contents
- [Introduction](#introduction)
- [Libraries Used](#libraries-used)
- [Setting Up the Virtual Environment](#setting-up-the-virtual-environment)
- [Installing Dependencies](#installing-dependencies)
- [Launching the Project](#launching-the-project)
- [Configuration](#configuration)
- [Seeding the Database](#seeding-the-database)
- [API Documentation](#api-documentation)

## Introduction

## Libraries Used
- `fastapi`: 0.65.0
- `sqlalchemy`: 1.4.26
- `passlib`: 1.7.4
- `bcrypt`: 3.2.0
- `python-dotenv`: 0.17.0
- `uvicorn`: 0.13.4

## Setting Up the Virtual Environment
To set up the virtual environment, follow these steps:

### Step 1: Install Miniconda or Anaconda
Install Miniconda or Anaconda on your system.

### Step 2: Create a New Environment
Create a new environment using the `environment.yml` file:

```bash
conda env create -f environment.yml
```

### Step 3: Activate the Environment
Activate the environment:

```bash
conda activate farmwise
```

### Step 4: Install Dependencies
Install the dependencies:

```bash
pip install -r requirements.txt
```

## Installing Dependencies
To install the dependencies, run the following command:

```bash
pip install -r requirements.txt
```

This will install all the required libraries and dependencies.

## Launching the Project
To launch the project, run the following command:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

This will start the FastAPI server on port 8000.

## Configuration
The project uses a `.env` file to store environment variables. You can create a `.env` file with the following variables:

- `ADMIN_ID`: the administrator ID
- `ADMIN_PASSWORD`: the administrator password
- `DATABASE_URL`: the database URL

Example `.env` file:

```bash
ADMIN_ID=admin
ADMIN_PASSWORD=password
DATABASE_URL=sqlite:///farmwise.db
```

Note that you should adapt these values to your specific needs.

## Seeding the Database
To seed the database with initial data, run the following command:

```bash
python main.py --seed
```

This will create the database tables and populate them with initial data.

## API Documentation
The API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs). You can use this documentation to explore the API endpoints and learn more about the available resources.

Authors : SmolakK (2024-2025) TO COMPLETE