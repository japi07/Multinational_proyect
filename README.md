# multinational-retail-data-centralisation

## Table of Contents

1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Description

This project focuses on data extraction, cleaning, and uploading to a PostgreSQL database. The project involves retrieving data from various sources, performing necessary cleaning operations, and storing the cleaned data.

### Aim of the Project

The aim is to automate all this process for a later analysis. This project also provides hands-on experience with data extraction from APIs, S3, and PDF files, data cleaning, and uploading to a database.

### What You Learned

- Extracting data from different sources including APIs, S3 buckets, and PDF files
- Cleaning data using Pandas
- Uploading data to a PostgreSQL database
- Automating  data extraction

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/japi07/multinational-retail-data-centralisation75.git
    ```
2. Navigate to the project directory:
    ```sh
    cd multinational_proyect
    ```
3. Create and activate a virtual environment:
    ```sh
    python3 -m venv new_test_env
    source new_test_env/bin/activate
    ```
4. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Ensure your PostgreSQL database is running and accessible.
2. Update the `db_creds.yaml` file with your database credentials.
3. Run the data extraction script:
    ```sh
    python3 data_extraction.py
    ```

### Primary Keys and Foreign Keys

We have updated the database schema to include primary and foreign keys to support a star-based database schema.

#### Primary Keys

- `dim_card_details`
  - Primary Key: `card_number`

- `dim_store_details`
  - Primary Key: `store_code`

- `dim_products`
  - Primary Key: `product_code`

#### Foreign Keys in `orders_table`

- `orders_table`
  - Foreign Key: `card_number` references `dim_card_details(card_number)`
  - Foreign Key: `store_code` references `dim_store_details(store_code)`
  - Foreign Key: `product_code` references `dim_products(product_code)`

### Query to Calculate the Busines Insights 

The following file contains the SQL queries to get the needed data insights for the bussines analysis.

SQL Queries: https://github.com/japi07/multinational-retail-data-centralisation75/blob/3f964c7db985a28ca6890a100d84d1b78c514c22/SQL%20Queries%20
