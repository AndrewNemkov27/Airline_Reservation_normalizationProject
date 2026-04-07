import mysql.connector
import csv

import xml.etree.ElementTree as ET
import pandas as pd
import os

from collections import defaultdict, deque

import time
from datetime import timedelta
import random

mydbase = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="WhiteTiger_11",
    database="air_reservation"
)

mycursor = mydbase.cursor(buffered=True)

# ----------(1) parse data - read in PNR.xml and IATA.txt into python
start = time.time()
# read in PNR.xml
# skips redoing read in if file already exists (time save)
overwrite = False
# True if enter "if"
# False if not enter "if"
if overwrite or not os.path.exists("PNR.csv") or os.path.getsize("PNR.csv") == 0:

    tree = ET.parse("PNR.xml")
    root = tree.getroot()

    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
    rows = root.findall(".//ss:Row", ns)
    data = []

    for i, row in enumerate(rows):
        cells = row.findall("ss:Cell", ns)

        values = []

        for cell in cells:
            data_tag = cell.find("ss:Data", ns)
            if data_tag is not None:
                values.append(data_tag.text)
            else:
                values.append("")

        # header row + rows with at least 10 cells
        if i == 0 or len(values) >= 10:
            data.append(values[:10])

    with open("PNR.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

df = pd.read_csv("PNR.csv")
# removed rows with all NA (at the bottom)
df = df.dropna(how="all")
# print(df.head())

# read in IATA.txt
airports = set()
with open("IATA.txt", "r") as file:
    for line in file:
        code = line.strip()
        if code:
            airports.add(code)

end = time.time()
print(f"Step 1 completed in {end - start:.2f} seconds")

# ----------(2) set up tables in MySQL - Airports, Passengers, Flights, Seats, Reservations

# ----------(3) INSERT into Passengers table
start = time.time()
data_to_insert = []

for _, row in df.iterrows():
    data_to_insert.append((
        row['firstname'],
        row['lastname'],
        row['address'],
        row['age'],
        row['source'],
        row['dest'],
        row['travelDate'],
        row['class'],
        row['bookingTime'],
        row['npass']
    ))

mycursor.executemany("""
INSERT IGNORE INTO Passengers
(firstname, lastname, address, age, source, dest, travelDate, class, bookingTime, npass)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", data_to_insert)
mydbase.commit()

end = time.time()
print(f"Step 3 completed in {end - start:.2f} seconds")

# ----------(4) GROUP and INSERT into Flights table
start = time.time()
unique_flights = df.groupby(['source', 'dest', 'travelDate']).size().reset_index()[['source', 'dest', 'travelDate']]
flight_data = []

for _, row in unique_flights.iterrows():
    flight_data.append((row['source'], row['dest'], row['travelDate']))

mycursor.executemany("""
INSERT IGNORE INTO Flights (origin, destination, travelDate)
VALUES (%s, %s, %s)
""", flight_data)
mydbase.commit()

end = time.time()
print(f"Step 4 completed in {end - start:.2f} seconds")

# ----------(5) Initialize data in Seats table


def chunked_execute(cursor, query, data, chunk_size=5000):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        cursor.executemany(query, chunk)
        mydbase.commit()


mycursor.execute("SELECT COUNT(1) FROM Seats")
if mycursor.fetchone()[0] == 0:

    start = time.time()

    # flight data
    mycursor.execute("SELECT flight_id FROM Flights")
    flights = mycursor.fetchall()

    # seat layout
    seat_layout = {
        'first': (10, 'ABCDE'),     # 10 rows, 5 columns
        'business': (20, 'ABCDE'),  # 20 rows, 5 columns
        'economy': (25, 'ABCDEF')   # 25 rows, 6 columns
    }

    # Optimization: pre-generate seats for all flights
    # compute seat combinations per class
    class_seats = []
    for cls, (rows_count, columns) in seat_layout.items():
        for row_num in range(1, rows_count + 1):
            for col_letter in columns:
                class_seats.append((cls, f"{row_num}{col_letter}"))

    # full insert list
    all_seats = [
        (flight[0], cls, seat_number)
        for flight in flights
        for cls, seat_number in class_seats
    ]

    # bulk insert with chunking
    chunked_execute(mycursor, """
        INSERT IGNORE INTO Seats (flight_id, class, seat_number)
        VALUES (%s, %s, %s)
    """, all_seats)

    # index for faster lookup
    mycursor.execute("""
        SELECT COUNT(1)
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE table_schema = DATABASE()
        AND table_name = 'Seats'
        AND index_name = 'idx_seats_full'
    """)
    if mycursor.fetchone()[0] == 0:
        mycursor.execute("""
            CREATE INDEX idx_seats_full
            ON Seats (flight_id, class, is_reserved, seat_number)
        """)
        mydbase.commit()

    end = time.time()
    print(f"Step 5 completed in {end - start:.2f} seconds")
else:
    print("Step 5 skipped")

# ----------(6) Sort Passengers by booking time
start = time.time()
mycursor.execute("""
SELECT passenger_id, firstname, lastname, source, dest, travelDate, class, bookingTime, npass
FROM Passengers
ORDER BY travelDate ASC, bookingTime ASC
""")
passengers = mycursor.fetchall()

end = time.time()
print(f"Step 6 completed in {end - start:.2f} seconds")

# ----------(7) Process each passenger - identify flight, check requested class seat, try upgrade, downgrade, split, cancel
mycursor.execute("SELECT COUNT(1) FROM Reservations")
if mycursor.fetchone()[0] == 0:
    start = time.time()

    # load all flights
    mycursor.execute("SELECT flight_id, origin, destination, travelDate FROM Flights")
    flights = mycursor.fetchall()
    flight_map = {(origin, dest, date): f_id for f_id, origin, dest, date in flights}

    class_order = ['economy', 'business', 'first']

    # Optimization 1: load all unreserved seats in ONE query
    mycursor.execute("""
        SELECT flight_id, class, seat_number
        FROM Seats
        WHERE is_reserved = 0
    """)
    all_seats = mycursor.fetchall()

    # build full cache upfront — no per-flight queries inside loop
    seats_cache = defaultdict(lambda: defaultdict(deque))
    for flight_id, cls, seat in all_seats:
        seats_cache[flight_id][cls].append(seat)

    # track reserved seats
    seats_to_update = defaultdict(set)
    reservations = []

    # process passengers
    for p in passengers:
        passenger_id, _, _, source, dest, travelDate, req_class, _, npass = p
        npass = int(npass)
        req_class = req_class.lower()

        if (source, dest, travelDate) not in flight_map:
            continue
        flight_id = flight_map[(source, dest, travelDate)]

        flight_seats = seats_cache[flight_id]
        seats_to_reserve = []

        # C1: requested class
        available = flight_seats[req_class]
        if len(available) >= npass:
            for _ in range(npass):
                seats_to_reserve.append((req_class, available.popleft()))
        else:
            class_idx = class_order.index(req_class)

            # C2: upgrade
            for higher_class in reversed(class_order[class_idx + 1:]):
                available = flight_seats[higher_class]
                if len(available) >= npass:
                    for _ in range(npass):
                        seats_to_reserve.append((higher_class, available.popleft()))
                    break

            # C3: downgrade
            if not seats_to_reserve:
                for lower_class in reversed(class_order[:class_idx]):
                    available = flight_seats[lower_class]
                    if len(available) >= npass:
                        for _ in range(npass):
                            seats_to_reserve.append((lower_class, available.popleft()))
                        break

            # C4: split
            if not seats_to_reserve:
                for cls in class_order:
                    available = flight_seats[cls]
                    while available and len(seats_to_reserve) < npass:
                        seats_to_reserve.append((cls, available.popleft()))
                    if len(seats_to_reserve) >= npass:
                        break

        # C5: cancel
        if not seats_to_reserve:
            continue

        for cls, seat in seats_to_reserve:
            seats_to_update[flight_id].add(seat)
            reservations.append((passenger_id, flight_id, cls, seat))

    # Optimization 2: one UPDATE per flight using "IN"
    for flight_id, seat_set in seats_to_update.items():
        seat_list = list(seat_set)
        # chunk "IN" clause
        chunk_size = 1000
        for i in range(0, len(seat_list), chunk_size):
            chunk = seat_list[i:i + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            mycursor.execute(f"""
                UPDATE Seats SET is_reserved = 1
                WHERE flight_id = %s AND seat_number IN ({placeholders})
            """, [flight_id] + chunk)
        mydbase.commit()

    # bulk insert
    if reservations:
        chunked_execute(mycursor, """
            INSERT IGNORE INTO Reservations (passenger_id, flight_id, class, seat_number)
            VALUES (%s, %s, %s, %s)
        """, reservations)

    end = time.time()
    print(f"Step 7 completed in {end - start:.2f} seconds")
else:
    print("Step 7 skipped")

# ----------(8) INSERT Check-in table
start = time.time()
# insert check-in rows in chunks
mycursor.execute("""
SELECT r.passenger_id, r.flight_id, f.travelDate
FROM Reservations r
JOIN Flights f ON r.flight_id = f.flight_id
""")
reservation_data = mycursor.fetchall()

checkin_rows = []
for passenger_id, flight_id, travelDate in reservation_data:
    # random if check-in
    checked_in = random.choice([True, False])

    # if true, assign time
    checkin_time = None
    if checked_in:
        hours_before = random.randint(1, 4)  # 1-4 hours before travelDate
        checkin_time = travelDate - timedelta(hours=hours_before)

    checkin_rows.append((passenger_id, flight_id, checked_in, checkin_time))

# insert into CheckIn table in batches
chunk_size = 5000
for i in range(0, len(checkin_rows), chunk_size):
    chunk = checkin_rows[i:i+chunk_size]
    mycursor.executemany("""
    INSERT IGNORE INTO CheckIn (passenger_id, flight_id, check_in, check_in_date)
    VALUES (%s, %s, %s, %s)
    """, chunk)
mydbase.commit()

end = time.time()
print(f"Step 8 completed in {end - start:.2f} seconds")

# get reservation table results --------------------
mycursor.execute("""
    SELECT * FROM Reservations
    LIMIT 10
""")
rows = mycursor.fetchall()
columns = [desc[0] for desc in mycursor.description]

print()
print("Top 10 Reservations:")
print(" | ".join(columns))
print("-" * 60)
for row in rows:
    print(" | ".join(str(val) for val in row))