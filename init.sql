CREATE DATABASE IF NOT EXISTS meteored;

USE meteored;

CREATE TABLE IF NOT EXISTS cities (
  city_id INT AUTO_INCREMENT PRIMARY KEY,
  city_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS successful_requests (
  id INT AUTO_INCREMENT PRIMARY KEY,
  date_request TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  city_id INT,
  dist VARCHAR(20),
  temperature VARCHAR(20),
  humidity VARCHAR(20),
  last_updated VARCHAR(20),
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE IF NOT EXISTS http_request (
  id INT AUTO_INCREMENT PRIMARY KEY,
  city_id INT,
  http_code INT NOT NULL,
  url_request  VARCHAR(255),
  date_request TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

