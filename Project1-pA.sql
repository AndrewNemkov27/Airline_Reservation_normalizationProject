DROP DATABASE air_reservation;
CREATE DATABASE IF NOT EXISTS air_reservation;
USE air_reservation;

CREATE TABLE IF NOT EXISTS Airports (
    iata_code CHAR(3) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS Passengers (
    passenger_id INT AUTO_INCREMENT PRIMARY KEY,
    firstname VARCHAR(50),
    lastname VARCHAR(50),
    address VARCHAR(255),
    age INT,
    source CHAR(3),
    dest CHAR(3),
    travelDate DATE,
    class VARCHAR(10),
    bookingTime TIME,
    npass INT
);

CREATE TABLE IF NOT EXISTS Flights (
    flight_id INT AUTO_INCREMENT PRIMARY KEY,
    origin CHAR(3),
    destination CHAR(3),
    travelDate DATE,
    first_class_seats INT DEFAULT 50,
    business_class_seats INT DEFAULT 100,
    economy_class_seats INT DEFAULT 150
);

CREATE TABLE IF NOT EXISTS Seats (
    seat_id INT AUTO_INCREMENT PRIMARY KEY,
    flight_id INT,
    class VARCHAR(10),
    seat_number VARCHAR(5),
    is_reserved BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (flight_id) REFERENCES Flights(flight_id)
);

CREATE TABLE IF NOT EXISTS Reservations (
    reservation_id INT AUTO_INCREMENT PRIMARY KEY,
    passenger_id INT,
    flight_id INT,
    class VARCHAR(10),
    seat_number VARCHAR(5),
    FOREIGN KEY (passenger_id) REFERENCES Passengers(passenger_id),
    FOREIGN KEY (flight_id) REFERENCES Flights(flight_id)
);

CREATE TABLE IF NOT EXISTS CheckIn (
    reservation_id INT AUTO_INCREMENT PRIMARY KEY,
    passenger_id INT NOT NULL,
    flight_id INT NOT NULL,
    check_in BOOLEAN NOT NULL DEFAULT FALSE,
    check_in_date DATE DEFAULT NULL,
    FOREIGN KEY (passenger_id) REFERENCES Passengers(passenger_id),
    FOREIGN KEY (flight_id) REFERENCES Flights(flight_id)
);