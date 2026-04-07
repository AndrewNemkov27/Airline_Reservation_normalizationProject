
    CREATE DATABASE IF NOT EXISTS air_reservation;
    USE air_reservation;

    DROP TABLE IF EXISTS Flights_norm;
    DROP TABLE IF EXISTS Passengers_norm;
    DROP TABLE IF EXISTS Reservations_norm;
    DROP TABLE IF EXISTS Seats_norm;

    CREATE TABLE Flights_norm (
        flight_id INT PRIMARY KEY,
        origin CHAR(3),
        destination CHAR(3),
        travelDate DATE
    );

    CREATE TABLE Passengers_norm (
        passenger_id INT PRIMARY KEY,
        firstname VARCHAR(50),
        lastname VARCHAR(50),
        address VARCHAR(255),
        age INT,
        bookingTime TIME,
        npass INT,
        requested_class VARCHAR(20)
    );

    CREATE TABLE Reservations_norm (
        reservation_id INT PRIMARY KEY,
        passenger_id INT
    );

    CREATE TABLE Seats_norm (
        seat_id INT PRIMARY KEY,
        flight_id INT,
        seat_class VARCHAR(20),
        seat_number VARCHAR(5),
        is_reserved BOOLEAN
    );

    LOAD DATA LOCAL INFILE 'Flights_norm.csv'
    INTO TABLE Flights_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Passengers_norm.csv'
    INTO TABLE Passengers_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Reservations_norm.csv'
    INTO TABLE Reservations_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Seats_norm.csv'
    INTO TABLE Seats_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS;