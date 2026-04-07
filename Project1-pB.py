import mysql.connector
import csv

import os
import pandas as pd
from itertools import combinations

mydbase = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="WhiteTiger_11",
    database="air_reservation",
    allow_local_infile=True

    # SHOW GLOBAL VARIABLES LIKE 'local_infile';
    # SET GLOBAL local_infile = 1;
    # SHOW GLOBAL VARIABLES LIKE 'local_infile';
)

mycursor = mydbase.cursor(buffered=True)

# ----------(1) CSV Data Import
file_name = "PNR_full.csv"

if not os.path.exists(file_name) or os.path.getsize(file_name) == 0:

    query = """
    SELECT
        s.seat_id,
        s.flight_id,
        f.origin,
        f.destination,
        f.travelDate,
        s.class AS seat_class,
        s.seat_number,
        s.is_reserved,
        r.reservation_id,
        p.passenger_id,
        p.firstname,
        p.lastname,
        p.address,
        p.age,
        p.bookingTime,
        p.npass,
        p.class AS requested_class
    FROM Seats s
    JOIN Flights f
        ON s.flight_id = f.flight_id
    LEFT JOIN Reservations r
        ON s.flight_id = r.flight_id
    AND s.seat_number = r.seat_number
    AND s.class = r.class
    LEFT JOIN Passengers p
        ON r.passenger_id = p.passenger_id
    """

    df_big = pd.read_sql(query, mydbase)
    df_big.to_csv(file_name, index=False)

    print("CSV created.")

else:
    print("CSV already exists. Skipping query.")

# CSV into pandas df
df = pd.read_csv(file_name)

# number of rows and columns
print()
print("Dataset Shape:")
print(f"rows: {df.shape[0]}, columns: {df.shape[1]}")

# sample records
print()
print("Sample Records (only reserved):")
print(df[df["is_reserved"] == 1].head())

# attribute data types
print()
print("Data Types:")
print(df.dtypes)

# ----------(2) Functional Dependency Identification
def get_user_input():
    print()
    # PNR_Full
    relation_name = input("Enter relation name: ")

    # seat_id, flight_id, origin, destination, travelDate, seat_class, seat_number, is_reserved, reservation_id, passenger_id, firstname, lastname, address, age, bookingTime, npass, requested_class
    attributes = input("Enter attributes (comma-separated): ")
    attributes = [attr.strip() for attr in attributes.split(",")]

    # seat_id->flight_id, seat_class, seat_number, is_reserved, reservation_id;flight_id->origin, destination, travelDate;passenger_id->firstname, lastname, address, age, bookingTime, npass, requested_class;reservation_id->passenger_id
    fd_input = input("Enter functional dependencies (e.g., A->B;C->D): ")

    # seat_id
    primary_keys = input("Enter primary key(s) (comma-separated): ")
    primary_keys = [key.strip() for key in primary_keys.split(",")]

    return relation_name, attributes, fd_input, primary_keys

def parse_fds(fd_input):
    fds = []
    for fd in fd_input.split(";"):
        left, right = fd.split("->")
        left = [x.strip() for x in left.split(",")]
        right = [x.strip() for x in right.split(",")]
        fds.append((left, right))
    return fds

def compute_closure(attrs, fds):
    closure = set(attrs)

    while True:
        updated = False
        for left, right in fds:
            if set(left).issubset(closure):
                for attr in right:
                    if attr not in closure:
                        closure.add(attr)
                        updated = True
        if not updated:
            break
    return closure

def find_partial(primary_keys, fds):
    if len(primary_keys) <= 1:
        return []

    result = []
    for left, right in fds:
        if set(left).issubset(set(primary_keys)) and set(left) != set(primary_keys):
            for attr in right:
                if attr not in primary_keys:
                    result.append((left, attr))
    return result

def find_transitive(primary_keys, fds):
    result = []
    for left, right in fds:
        if not set(left).issubset(set(primary_keys)):
            for attr in right:
                if attr not in primary_keys:
                    result.append((left, attr))
    return result

def find_candidate_keys(attributes, fds):
    all_attrs = set(attributes)
    keys = []

    for r in range(1, len(attributes) + 1):
        for combo in combinations(attributes, r):
            if compute_closure(combo, fds) == all_attrs:
                if not any(set(k).issubset(combo) for k in keys):
                    keys.append(combo)
    return keys


relation_name, attributes, fd_input, primary_keys = get_user_input()

fds = parse_fds(fd_input)

print()
print("Closures:")
for left, _ in fds:
    print(left, "->", compute_closure(left, fds))

print()
print("Partial Dependencies:", find_partial(primary_keys, fds))

print()
print("Transitive Dependencies:", find_transitive(primary_keys, fds))

print()
print("Candidate Keys:")
for key in find_candidate_keys(attributes, fds):
    print(key)

# ----------(3) Normalization Process
def check_1NF():
    print("\nChecking 1NF: Already in 1NF (No multi-valued attributes)")

def check_2NF(attributes, primary_keys, fds):
    violations = []

    if len(primary_keys) <= 1:
        print("Checking 2NF: Automatically in 2NF (No composite key)")
        return violations

    for left, right in fds:
        if set(left).issubset(set(primary_keys)) and set(left) != set(primary_keys):
            for attr in right:
                if attr not in primary_keys:
                    violations.append((left, attr))
                    print(f"Checking 2NF: Partial Dependency Found: {', '.join(left)} -> {attr}")

    if not violations:
        print("Checking 2NF: No 2NF violations found")

    return violations

def check_3NF(attributes, primary_keys, fds):
    violations = []

    print("Checking 3NF:")

    for left, right in fds:
        bad_attrs = []

        for attr in right:
            if attr not in primary_keys and not set(left).issubset(set(primary_keys)):
                violations.append((left, attr))
                bad_attrs.append(attr)

        if bad_attrs:
            print(f"Transitive Dependency Found: {', '.join(left)} -> {', '.join(bad_attrs)}")

    if not violations:
        print("No 3NF violations found")

    return violations

def decompose(attributes, primary_keys, violations_2NF, violations_3NF, fds, candidate_keys):
    print()
    print("Final BCNF Tables:")

    new_tables = {}

    # group 2NF + 3NF violations by left side
    for left, attr in violations_2NF + violations_3NF:
        left_tuple = tuple(left)

        if left_tuple not in new_tables:
            new_tables[left_tuple] = list(left)

        if attr not in new_tables[left_tuple]:
            new_tables[left_tuple].append(attr)

    # BCNF
    candidate_key_sets = [set(key) for key in candidate_keys]

    for left, right in fds:
        if set(left) not in candidate_key_sets:
            left_tuple = tuple(left)

            if left_tuple not in new_tables:
                new_tables[left_tuple] = list(left)

            for attr in right:
                if attr not in new_tables[left_tuple]:
                    new_tables[left_tuple].append(attr)

    final_tables = list(new_tables.values())

    used_attrs = set()
    for table in final_tables:
        used_attrs.update(table)

    remaining = [attr for attr in attributes if attr not in used_attrs or attr in primary_keys]
    if remaining:
        final_tables.append(remaining)

    for i, table in enumerate(final_tables, 1):
        print(f"Table{i}({', '.join(table)})")

    return final_tables


check_1NF()

violations_2NF = check_2NF(attributes, primary_keys, fds)
violations_3NF = check_3NF(attributes, primary_keys, fds)

candidate_keys = find_candidate_keys(attributes, fds)

tables_bcnf = decompose(attributes, primary_keys, violations_2NF, violations_3NF, fds, candidate_keys)

# ----------(4) SQL Script Generation
sql_file = "Project1-pB.sql"

flights_csv = "Flights_norm.csv"
passengers_csv = "Passengers_norm.csv"
reservations_csv = "Reservations_norm.csv"
seats_csv = "Seats_norm.csv"

# read original big csv once
df = pd.read_csv("PNR_full.csv")

flights_df = df[["flight_id", "origin", "destination", "travelDate"]].drop_duplicates()
passengers_df = df[["passenger_id", "firstname", "lastname", "address", "age", "bookingTime", "npass", "requested_class"]].drop_duplicates()
reservations_df = df[["reservation_id", "passenger_id"]].dropna().drop_duplicates()
seats_df = df[["seat_id", "flight_id", "seat_class", "seat_number", "is_reserved"]].drop_duplicates()

# create csv only if missing or empty
if not os.path.exists(flights_csv) or os.path.getsize(flights_csv) == 0:
    flights_df.to_csv(flights_csv, index=False)

if not os.path.exists(passengers_csv) or os.path.getsize(passengers_csv) == 0:
    passengers_df.to_csv(passengers_csv, index=False)

if not os.path.exists(reservations_csv) or os.path.getsize(reservations_csv) == 0:
    reservations_df.to_csv(reservations_csv, index=False)

if not os.path.exists(seats_csv) or os.path.getsize(seats_csv) == 0:
    seats_df.to_csv(seats_csv, index=False)

# create whole sql only if missing or empty
if not os.path.exists(sql_file) or os.path.getsize(sql_file) == 0:
    with open(sql_file, "w", encoding="utf-8") as f:
        f.write("""
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
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Passengers_norm.csv'
    INTO TABLE Passengers_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Reservations_norm.csv'
    INTO TABLE Reservations_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;

    LOAD DATA LOCAL INFILE 'Seats_norm.csv'
    INTO TABLE Seats_norm
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;
    """)

    print("SQL script generated into file Project1-pB.sql")
else:
    print("Project1-pB.sql already exists. Skipping generation.")

# ----------(5) Database Creation and Query Interface
try:
    mycursor.execute("SELECT 1 FROM Flights_norm LIMIT 1")
    already_loaded = mycursor.fetchone() is not None
except mysql.connector.Error:
    already_loaded = False

if already_loaded:
    print("Tables already populated. Skipping SQL execution.")
else:
    with open("Project1-pB.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()

    # use semicolon to execute pB sql part by part
    statements = sql_script.split(";")
    for statement in statements:
        statement = statement.strip()
        if statement:
            mycursor.execute(statement)

    mydbase.commit()
    print("SQL script executed.")

# table data check
print()
print("Table Row Counts:")
tables = ["Flights_norm", "Passengers_norm", "Reservations_norm", "Seats_norm"]

for table in tables:
    mycursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = mycursor.fetchone()[0]
    print(f"{table}: {count} rows")

# user interactive interface below
print("\nSimple Query Interface")

# show tables for easier querying
tables = ["Flights_norm", "Passengers_norm", "Reservations_norm", "Seats_norm"]
print("\nAvailable Tables:")
for table in tables:
    print(f"{table}")
print()
print("Type 'exit' to quit")

while True:
    query = input("\nEnter SQL query: ")
    if query.lower() == "exit":
        print("Exiting query interface.")
        break

    try:
        mycursor.execute(query)
        if query.strip().lower().startswith("select"):
            rows = mycursor.fetchall()
            columns = [desc[0] for desc in mycursor.description]
            print("\n" + " | ".join(columns))
            print("-" * 60)

            if rows:
                for row in rows:
                    print(" | ".join(str(value) for value in row))
            else:
                print("No rows found.")
        else:
            mydbase.commit()
            print("Query executed successfully.")

    except mysql.connector.Error as e:
        print("Error:", e)

# SELECT * FROM Flights_norm LIMIT 5;
# SELECT COUNT(*) FROM Seats_norm WHERE is_reserved = 1;