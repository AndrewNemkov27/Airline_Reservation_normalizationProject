# Airline Reservation System & Database Normalization (CMPS 664)

## Overview
This project implements an airline reservation system and demonstrates the full process of database normalization using real data.

The workflow begins with raw passenger reservation data and progresses through:
- data parsing and preprocessing
- relational database design and population
- functional dependency analysis
- normalization from **1NF to BCNF**
- SQL schema generation
- database execution and querying

The objective is to transform a large, unstructured dataset into a well-designed and fully normalized relational database.

---

## Project Structure

### **Part A – Airline Reservation System**
- Parses passenger data from XML
- Builds relational schema in MySQL:
  - *Airports, Passengers, Flights, Seats, Reservations, Check-In*
- Implements reservation logic:
  - *first-come-first-served*
  - *automatic upgrades/downgrades*
  - *multiple passenger handling*
- Populates database with processed data

---

### **Part B – Database Normalization Tool**

- Imports structured dataset into pandas
- Performs functional dependency analysis
- Identifies candidate keys and dependency types
- Normalizes dataset from **1NF → 2NF → 3NF → BCNF**
- Generates SQL scripts and normalized tables
- Loads data into MySQL
- Provides an interactive query interface

---

## File Descriptions

---

### **Project1-pA.py**
This file implements the core *airline reservation system* and populates the initial database.

- **Step 1 – Data Parsing**  
  Parses *PNR.xml* using **xml.etree.ElementTree**, converts data into CSV using **csv** and **pandas**, and loads airport codes from *IATA.txt*.

- **Step 2 – Database Setup**  
  Connects to MySQL using **mysql.connector** and prepares relational tables.

- **Step 3 – Passenger Insertion**  
  Inserts passenger data efficiently using **executemany**.

- **Step 4 – Flight Generation**  
  Uses **pandas.groupby** to generate unique flights and inserts them into the database.

- **Step 5 – Seat Initialization**  
  Dynamically generates seat layouts for *First, Business,* and *Economy* classes and performs optimized batch inserts.

- **Step 6 – Passenger Sorting**  
  Orders passengers by *travel date* and *booking time*.

- **Step 7 – Reservation Logic**  
  Implements seat assignment rules including:
  - *requested class allocation*
  - *automatic upgrades/downgrades*
  - *seat splitting across classes*
  - *cancellation if no seats available*  
  Uses **defaultdict** and **deque** for efficient processing.

- **Step 8 – Check-In Simulation**  
  Assigns random check-in status and times using **random** and **datetime**.

**Summary:**  
Builds and populates the **operational airline reservation database**.

---

### **Project1-pB.py**
This file performs *data analysis, normalization, and database restructuring* based on Part A.

- **Step 1 – CSV Data Import**  
  Extracts data into *PNR_full.csv*, loads with **pandas**, and displays dataset structure.

- **Step 2 – Functional Dependency Analysis**  
  Accepts functional dependencies and:
  - computes *attribute closures*
  - identifies *partial dependencies*
  - identifies *transitive dependencies*
  - determines *candidate keys* using **itertools.combinations**

- **Step 3 – Normalization**  
  Verifies and enforces:
  - **1NF**
  - **2NF**
  - **3NF**  
  Performs **BCNF decomposition**.

- **Step 4 – SQL Script Generation**  
  Uses **pandas** to:
  - generate normalized datasets
  - create CSV files
  - produce SQL script (*Project1-pB.sql*)

- **Step 5 – Database Creation & Query Interface**  
  Executes SQL script and provides an interactive interface supporting:
  - **SELECT**
  - **INSERT**
  - **UPDATE**
  - **DELETE**

**Summary:**  
Produces a **fully normalized relational schema** and enables querying.

---

## Files Not Included
Due to file size limitations, the following files are not included:

- *Original datasets*: `PNR.xml`, `iata.txt`
- *Intermediate datasets*: `PNR.csv`, `PNR_full.csv`
- *Normalized CSV files*:  
  `Seats_norm.csv`, `Reservations_norm.csv`, `Flights_norm.csv`, `Passengers_norm.csv`

These files can be regenerated using the provided scripts.

---

## Python Libraries Used

### **Part A**
- `mysql.connector`
- `csv`
- `xml.etree.ElementTree`
- `pandas`
- `os`
- `collections` (*defaultdict, deque*)
- `time`
- `datetime`
- `random`

### **Part B**
- `mysql.connector`
- `csv`
- `os`
- `pandas`
- `itertools` (*combinations*)

---

## Important Configuration

To run `Project1-pB.py`, **local file loading must be enabled**.

### **Python**
```python
mysql.connector.connect(..., allow_local_infile=True)
