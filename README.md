Project Requirements:
Java 8 or higher
ANT build tool
SQLITE3 database (can be downloaded for free - is an open source database management system)

Project Description:

This is a simple ETL project that loads data from the three included CSV files (flights.csv, airlines.csv, and airports.csv) into an SQLite database using JDBC. To create the database, The database schema is as follows:

airlines (airline id, airline code, airline name)
airports (airport id, airport code, airport name, city, state)

flights(flight id, airline id, flight num, origin airport id,
dest airport id, departure dt, depart diff, arrival dt,
arrival diff, cancelled, carrier delay, weather delay, air traffic delay, security delay)

After the database is created, a set of pre-defined queries were written in ETLQuery.java. The queries retrieve the answers to the questions presented in the ETL Query List pdf file.

Running Instructions:
To perform ETL and create the initial database, run the following command:
./import \
airports.csv \
airlines.csv \
flights.csv \
data.db

To run the pre-defined queries, run the following command:

./query \
data.db \
<Query Number> \
<Query Argument 1> \
<Query Argument 2> \
<Query Argument 3 ...> /

The query numbers correspond to the designated queries in the ETL Query List pdf document. 