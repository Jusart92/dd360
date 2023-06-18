## Weather Data Extraction and Processing

This project aims to extract weather data from a website and process it for further analysis and storage. The project utilizes a combination of web scraping, data processing, and database integration to achieve the desired outcome.

### Requirements

To run the project, you need to have Docker and Docker Compose installed on your system.

### Installation

1. Clone the repository from GitHub: [repository link](https://github.com/Jusart92/dd360.git).

2. Navigate to the project directory in your terminal.

3. Run the following command to start the project using Docker Compose:
   ```
   docker-compose up
   ```

4. Wait for the containers to build and start. Once everything is up and running, you can access the Airflow web interface at `http://localhost:8080`.

5. In the Airflow web interface, navigate to the "Admin" -> "Connections" page and create a new connection with the following details:
    Conn Id: mysql_conn
    Conn Type: MySQL
    Host: database
    Schema: meteored
    Login: root
    Password: dd360
    (detail on docker-compose.yaml)


### Usage

1. In the Airflow web interface, navigate to the "DAGs" page and enable the "weather_scraper" DAG.

2. Monitor the DAG execution and check the generated JSON files, database entries, and Parquet files in their respective directories.

### Process Overview

1. The scraper script (`scraper.py`) runs every hour to extract weather data from specific websites. It collects data such as distance to the weather station, date and time of update, current temperature, and relative humidity.

2. The extracted data is saved in JSON files, with each file containing information from a specific website and timestamp.

3. The data is then loaded into a MySQL database, where the tables store the HTTP response codes, weather data, and city information.

4. Finally, the data is exported to Parquet files, partitioned by the execution timestamp. Each partition contains a summary of the available data, including the maximum, minimum, and average temperature and humidity, as well as the last update time for each city.

### Results

The project generates the following output files:

- JSON files: Contains the raw weather data extracted from the websites. Each file represents a specific website and timestamp.

- CSV files: Store the cleaned and processed data from the JSON files. These files are loaded into the MySQL database.

- Parquet files: Partitioned by execution timestamp, these files provide a summary of the weather data, including temperature and humidity statistics for each city.

### Possible Improvements

- Implement data validation and integrity checks to ensure the accuracy and consistency of the extracted and processed data.

- Incorporate data visualization tools to create interactive dashboards for visualizing the weather data.

- Enhance error handling and logging mechanisms to identify and handle any potential issues during the data extraction and processing pipeline.

### Conclusion

This project demonstrates a data extraction and processing pipeline for weather data. By utilizing web scraping, database integration, and Parquet file export, the project provides a scalable and automated solution for obtaining and analyzing weather information.