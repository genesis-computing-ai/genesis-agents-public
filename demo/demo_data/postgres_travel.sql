-- to run this:
-- psql postgres -f ./demo/demo_data/postgres_travel.sql 

-- First connect to postgres database
\c postgres;

-- Drop the database if it exists and create it fresh
DROP DATABASE IF EXISTS travel;
CREATE DATABASE travel;

-- Connect to the new travel database
\c travel;

-- Create schemas
CREATE SCHEMA flights;
CREATE SCHEMA trains;

-- Create tables in flights schema
CREATE TABLE flights.airports (
    airport_id SERIAL PRIMARY KEY,
    iata_code CHAR(3) UNIQUE NOT NULL,
    airport_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    timezone VARCHAR(50)
);

CREATE TABLE flights.planes (
    plane_id SERIAL PRIMARY KEY,
    aircraft_type VARCHAR(50) NOT NULL,
    manufacturer VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL,
    capacity INT NOT NULL,
    first_class_seats INT,
    business_seats INT,
    economy_seats INT,
    max_range_km INT
);

CREATE TABLE flights.flight_schedules (
    schedule_id SERIAL PRIMARY KEY,
    flight_number VARCHAR(10) NOT NULL,
    departure_airport_id INT REFERENCES flights.airports(airport_id),
    arrival_airport_id INT REFERENCES flights.airports(airport_id),
    plane_id INT REFERENCES flights.planes(plane_id),
    departure_time TIME NOT NULL,
    arrival_time TIME NOT NULL,
    operating_days VARCHAR(7), -- e.g., '1234567' for all days
    base_price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT valid_airports CHECK (departure_airport_id != arrival_airport_id)
);

-- Create tables in trains schema
CREATE TABLE trains.stations (
    station_id SERIAL PRIMARY KEY,
    station_code VARCHAR(5) UNIQUE NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    number_of_platforms INT,
    has_wifi BOOLEAN DEFAULT false,
    has_parking BOOLEAN DEFAULT false
);

CREATE TABLE trains.trains (
    train_id SERIAL PRIMARY KEY,
    train_number VARCHAR(10) UNIQUE NOT NULL,
    train_type VARCHAR(50) NOT NULL, -- e.g., 'High Speed', 'Regional', 'Express'
    total_carriages INT NOT NULL,
    first_class_carriages INT,
    second_class_carriages INT,
    max_speed_kmh INT,
    has_dining_car BOOLEAN DEFAULT false
);

CREATE TABLE trains.train_schedules (
    schedule_id SERIAL PRIMARY KEY,
    train_id INT REFERENCES trains.trains(train_id),
    departure_station_id INT REFERENCES trains.stations(station_id),
    arrival_station_id INT REFERENCES trains.stations(station_id),
    departure_time TIME NOT NULL,
    arrival_time TIME NOT NULL,
    operating_days VARCHAR(7), -- e.g., '1234567' for all days
    base_fare DECIMAL(10, 2) NOT NULL,
    distance_km INT,
    CONSTRAINT valid_stations CHECK (departure_station_id != arrival_station_id)
);

-- Add some indexes for better query performance
CREATE INDEX idx_flight_schedules_flight_number ON flights.flight_schedules(flight_number);
CREATE INDEX idx_airports_iata ON flights.airports(iata_code);
CREATE INDEX idx_train_schedules_train_id ON trains.train_schedules(train_id);
CREATE INDEX idx_stations_code ON trains.stations(station_code);

-- Now insert sample data
-- Flights schema data
INSERT INTO flights.airports (iata_code, airport_name, city, country, latitude, longitude, timezone) VALUES
    ('SFO', 'San Francisco International', 'San Francisco', 'USA', 37.7749, -122.4194, 'America/Los_Angeles'),
    ('JFK', 'John F Kennedy International', 'New York', 'USA', 40.7128, -74.0060, 'America/New_York'),
    ('LHR', 'London Heathrow', 'London', 'UK', 51.5074, -0.1278, 'Europe/London'),
    ('CDG', 'Charles de Gaulle', 'Paris', 'France', 48.8566, 2.3522, 'Europe/Paris'),
    ('NRT', 'Narita International', 'Tokyo', 'Japan', 35.6762, 139.6503, 'Asia/Tokyo');

INSERT INTO flights.planes (aircraft_type, manufacturer, model, capacity, first_class_seats, business_seats, economy_seats, max_range_km) VALUES
    ('Wide Body', 'Boeing', '787-9', 290, 20, 40, 230, 14140),
    ('Wide Body', 'Airbus', 'A350-900', 325, 24, 45, 256, 15000),
    ('Narrow Body', 'Boeing', '737-800', 180, 0, 16, 164, 5765),
    ('Wide Body', 'Boeing', '777-300ER', 396, 30, 48, 318, 13650),
    ('Narrow Body', 'Airbus', 'A320neo', 180, 0, 20, 160, 6300);

INSERT INTO flights.flight_schedules (flight_number, departure_airport_id, arrival_airport_id, plane_id, departure_time, arrival_time, operating_days, base_price) VALUES
    ('UA123', 1, 2, 1, '08:00', '16:30', '1234567', 450.00),
    ('BA456', 3, 4, 2, '09:15', '11:45', '1234567', 220.00),
    ('JL789', 5, 1, 4, '23:00', '16:30', '1234567', 890.00),
    ('AF234', 4, 3, 2, '14:30', '15:45', '1234567', 195.00),
    ('UA456', 2, 5, 1, '13:15', '17:45', '1234567', 780.00);

-- Trains schema data
INSERT INTO trains.stations (station_code, station_name, city, country, number_of_platforms, has_wifi, has_parking) VALUES
    ('GCT', 'Grand Central Terminal', 'New York', 'USA', 44, true, true),
    ('KINGS', 'Kings Cross', 'London', 'UK', 12, true, true),
    ('GARE', 'Gare du Nord', 'Paris', 'France', 29, true, true),
    ('TOKYO', 'Tokyo Station', 'Tokyo', 'Japan', 28, true, true),
    ('CENT', 'Central Station', 'Amsterdam', 'Netherlands', 15, true, true);

INSERT INTO trains.trains (train_number, train_type, total_carriages, first_class_carriages, second_class_carriages, max_speed_kmh, has_dining_car) VALUES
    ('EUR100', 'High Speed', 8, 2, 6, 300, true),
    ('TGV200', 'High Speed', 10, 3, 7, 320, true),
    ('REG300', 'Regional', 6, 1, 5, 160, false),
    ('BULLET1', 'High Speed', 12, 4, 8, 320, true),
    ('EXP500', 'Express', 8, 2, 6, 200, true);

INSERT INTO trains.train_schedules (train_id, departure_station_id, arrival_station_id, departure_time, arrival_time, operating_days, base_fare, distance_km) VALUES
    (1, 2, 3, '08:00', '11:15', '1234567', 120.00, 491),
    (2, 3, 5, '09:30', '13:45', '1234567', 150.00, 544),
    (3, 1, 2, '10:00', '16:30', '12345', 80.00, 350),
    (4, 4, 1, '07:15', '12:45', '1234567', 200.00, 450),
    (5, 5, 2, '14:30', '19:15', '1234567', 135.00, 522);
