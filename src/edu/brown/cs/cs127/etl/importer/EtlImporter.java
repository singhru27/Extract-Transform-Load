package edu.brown.cs.cs127.etl.importer;

import au.com.bytecode.opencsv.CSVReader; //for the CSVReader objects

import java.io.FileNotFoundException;
import java.io.FileReader; //(for file reader)
import java.io.IOException;
import java.sql.*; //(for jdbc)
import java.util.Objects; //(for various useful Java functions)
import java.util.ArrayList; //If you are planning to use ArrayLists
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.*;

public class EtlImporter {
	/**
	 * You are only provided with a main method, but you may create as many new
	 * methods, other classes, etc as you want: just be sure that your application
	 * is runnable using the correct shell scripts.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("This application requires exactly four parameters: "
					+ "the path to the airports CSV, the path to the airlines CSV, "
					+ "the path to the flights CSV, and the full path where you would "
					+ "like the new SQLite database to be written to.");
			System.exit(1);
		}

		String AIRPORTS_FILE = args[0];
		String AIRLINES_FILE = args[1];
		String FLIGHTS_FILE = args[2];
		String DB_FILE = args[3];

		/*
		 * For more sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 * --- // INITIALIZE THE CONNECTION
		 */

		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);

		/*
		 */ // ENABLE FOREIGN KEY CONSTRAINT CHECKING
		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
		// Speed up INSERTs
		stat.executeUpdate("PRAGMA synchronous = OFF;");
		stat.executeUpdate("PRAGMA journal_mode = MEMORY;");

		/*
		 * Deleting existing tables to enable the creation of a fresh database
		 */
		stat.executeUpdate("DROP TABLE IF EXISTS flights;");
		stat.executeUpdate("DROP TABLE IF EXISTS airlines;");
		stat.executeUpdate("DROP TABLE IF EXISTS airports;");
		// Creating the airline table
		stat.executeUpdate(
				"CREATE TABLE airlines (airline_id INTEGER PRIMARY KEY AUTOINCREMENT, airline_code VARCHAR(7) UNIQUE, airline_name VARCHAR(200));");
		// Creating the airports table
		stat.executeUpdate(
				"CREATE TABLE airports (airport_id INTEGER PRIMARY KEY AUTOINCREMENT, airport_code CHAR(3) UNIQUE, airport_name VARCHAR(200), city VARCHAR(200), state VARCHAR(200));");
		// Creating the flights table
		stat.executeUpdate(
				"CREATE TABLE flights (flight_id INTEGER PRIMARY KEY AUTOINCREMENT, airline_id INTEGER NOT NULL, flight_num INTEGER NOT NULL, origin_airport_id INTEGER NOT NULL, dest_airport_id INTEGER NOT NULL, "
						+ "departure_date TEXT NOT NULL, departure_time TIME NOT NULL, departure_diff INTEGER NOT NULL, arrival_date TEXT NOT NULL, arrival_time im NOT NULL, arrival_diff INTEGER NOT NULL, cancelled BOOLEAN NOT NULL, carrier_delay INTEGER NOT NULL, weather_delay INTEGER NOT NULL, air_traffic_delay INTEGER NOT NULL, "
						+ "security_delay INTEGER NOT NULL, FOREIGN KEY (airline_id) REFERENCES airlines(airline_id) ON DELETE CASCADE ON UPDATE NO ACTION, FOREIGN KEY(origin_airport_id) REFERENCES airports(airport_id) ON DELETE CASCADE ON UPDATE "
						+ "NO ACTION, FOREIGN KEY (dest_airport_id) REFERENCES airports(airport_id) ON DELETE CASCADE ON UPDATE NO ACTION,"
						+ "CHECK(cancelled >= 0 and cancelled <=1), CHECK (carrier_delay >= 0), CHECK (weather_delay >= 0), CHECK (air_traffic_delay>=0), check (security_delay >= 0), check(arrival_date >= departure_date), CHECK(datetime(arrival_date||arrival_time, arrival_diff||' minutes') > datetime(departure_date||departure_time, departure_diff|| ' minutes')));");

		/*
		 * Initial pass to load all database tables. The Airports table will need to be
		 * updated with the city and state once the flights table is loaded in
		 */
		HashMap<String, Integer> airlineCodeToID = loadAirlines(AIRLINES_FILE, conn);
		HashMap<String, Integer> airportCodeToID = loadAirports(AIRPORTS_FILE, conn);
		HashMap<Integer, String[]> airportIDToCity = loadFlights(FLIGHTS_FILE, conn, airlineCodeToID, airportCodeToID);
		/*
		 * Second pass to populate the airports database with city and state
		 * information, which is pulled from the flights CSV in the loadFlights method
		 */
		updateAirportCityState (airportIDToCity, conn);
	}

	/*
	 * Function Description: Loads the airports database with city and state
	 * information from the flights CSV file Inputs: airportCodeToCity - HashMap
	 * mapping airport IDs to a string containing city and state names, conn -
	 * Connection point to the database Returns: null
	 */

	private static void updateAirportCityState (HashMap<Integer, String[]> airportIDToCity, Connection conn) throws SQLException {
		PreparedStatement prep = conn.prepareStatement ("UPDATE airports SET city = ?, state = ? WHERE airport_id = ?");
		
		/*
		 * Iterating through each element in the HashMap
		 */
		Iterator locationIterator = airportIDToCity.entrySet().iterator();
		
		while (locationIterator.hasNext() ) {
			Map.Entry mapElement = (Map.Entry)locationIterator.next();
			int airportID = (int)mapElement.getKey();
			String [] airportInfo = (String [])mapElement.getValue();
			String city = airportInfo[0];
			String state = airportInfo[1];
			prep.setString(1, city);
			prep.setString(2,  state);
			prep.setInt(3, airportID);
			prep.addBatch();
		}
		
		/*
		 * Executing the current batch and updating the database
		 */
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);
	}

	/*
	 * Function Description: Loads the flight database into database, returns a
	 * HashMap with key = airport_ID, value = [city, state] Inputs: Flights_File -
	 * String representing the flights CSV, conn - JDBC connection point,
	 * airlineCodeToID - HashMap mapping from airline code to our autoincremented
	 * airlineID Returns: HashMap with key = Airport Codes and values = String []
	 * with String[0] = city and String [1] = State
	 */
	private static HashMap<Integer, String[]> loadFlights(String FLIGHTS_FILE, Connection conn,
			HashMap<String, Integer> airlineCodeToID, HashMap<String, Integer> airportCodeToID)
			throws FileNotFoundException, SQLException, IOException, ParseException {
		/*
		 * Reading in the data from the flights file into the database. Each element
		 * from the CSV is a list of strings
		 */
		CSVReader reader_flights = new CSVReader(new FileReader(FLIGHTS_FILE));
		String[] nextLineFlights;
		HashMap<Integer, String[]> airportIdToCity = new HashMap<>();

		/*
		 * Reading each line of the CSV, creating batch input
		 */
		PreparedStatement prep = conn.prepareStatement(
				"INSERT OR IGNORE INTO flights (airline_id, flight_num, origin_airport_id, dest_airport_id, departure_date, departure_time, departure_diff, arrival_date, arrival_time, arrival_diff, cancelled, carrier_delay, weather_delay, air_traffic_delay, security_delay) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?)");
		while ((nextLineFlights = reader_flights.readNext()) != null) {

			/*
			 * Retrieving the airport and airline codes from the current row in the CSV
			 * file. To be used to check against foreign key constraints.
			 */
			String originAirportCode = nextLineFlights[2];
			String destAirportCode = nextLineFlights[5];
			String airlineCode = nextLineFlights[0];
			/*
			 * Checking foreign key constraints. If the constraints are not satisfied, we
			 * skip adding this CSV row into the database
			 */
			if (airlineCodeToID.get(airlineCode) == null) {
				continue;
			}
			if (airportCodeToID.get(originAirportCode) == null) {
				continue;
			}
			if (airportCodeToID.get(destAirportCode) == null) {
				continue;
			}
			/*
			 * Setting the attributes for the current batch
			 */
			prep.setInt(1, airlineCodeToID.get(airlineCode));
			prep.setInt(2, Integer.valueOf(nextLineFlights[1]));
			prep.setInt(3, airportCodeToID.get(originAirportCode));
			prep.setInt(4, airportCodeToID.get(destAirportCode));
			prep.setString(5, returnCorrectDateString(nextLineFlights[8]));
			prep.setString(6, returnCorrectTimeString(nextLineFlights[9]));
			prep.setInt(7, Integer.valueOf(nextLineFlights[10]));
			prep.setString(8, returnCorrectDateString(nextLineFlights[11]));
			prep.setString(9, returnCorrectTimeString(nextLineFlights[12]));
			prep.setInt(10, Integer.valueOf(nextLineFlights[13]));
			prep.setInt(11, Integer.valueOf(nextLineFlights[14]));
			prep.setInt(12, Integer.valueOf(nextLineFlights[15]));
			prep.setInt(13, Integer.valueOf(nextLineFlights[16]));
			prep.setInt(14, Integer.valueOf(nextLineFlights[17]));
			prep.setInt(15, Integer.valueOf(nextLineFlights[18]));

			/*
			 * Adding city state information to HashMap
			 */
			String originCity = nextLineFlights[3];
			String originState = nextLineFlights[4];
			String destCity = nextLineFlights[6];
			String destState = nextLineFlights[7];
			String[] origin = new String[2];
			String[] destination = new String[2];
			origin[0] = originCity;
			origin[1] = originState;
			destination[0] = destCity;
			destination[1] = destState;
			airportIdToCity.put(airportCodeToID.get(originAirportCode), origin);
			airportIdToCity.put(airportCodeToID.get(destAirportCode), destination);

			/*
			 * Adding to batch
			 */
			prep.addBatch();
		}

		/*
		 * Executing the current batch and updating the database
		 */
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);
		reader_flights.close();
		return airportIdToCity;
	}

	/*
	 * Function Description: This function partially creates the Airports relation.
	 * It leaves the city and state as NULL, but fills the remainder Inputs:
	 * -AIRPORTS FILE CSV file, and a connection point for the database Returns: - A
	 * HashMap of Airport code to Airport IDs
	 */

	private static HashMap<String, Integer> loadAirports(String AIRPORTS_FILE, Connection conn)
			throws FileNotFoundException, SQLException, IOException {
		/*
		 * Reading in the data from the airlines file the database. Each element is a
		 * list of strings, where the first element is the airline code and the second
		 * element is the airline name
		 */
		CSVReader reader_ap = new CSVReader(new FileReader(AIRPORTS_FILE));
		String[] nextLine_ap;
		// Counter to be used for lookup when loading data into the array
		int j = 1;
		HashMap<String, Integer> airport_code_to_id = new HashMap<>();
		PreparedStatement prep = conn
				.prepareStatement("INSERT OR IGNORE INTO airports (airport_code, airport_name) VALUES (?, ?)");

		/*
		 * Adding airport_code and airport_name to the prepared statement, and also
		 * including into the HashMap
		 */
		while ((nextLine_ap = reader_ap.readNext()) != null) {
			prep.setString(1, nextLine_ap[0]);
			prep.setString(2, nextLine_ap[1]);
			prep.addBatch();
			// Adding to a HashMap for future reference
			airport_code_to_id.put(nextLine_ap[0], j);
			j += 1;
		}
		/*
		 * Executing the prepared statement and returning the HashMap
		 */
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);
		reader_ap.close();
		return airport_code_to_id;
	}

	/*
	 * Function Description: This function fully creates the Airlines relation, and
	 * returns a HashMap mapping from the airline codes to the airline IDs Inputs: -
	 * AIRLINES FILE CSV file, and a connection point for the database Returns: - A
	 * HashMap of Airline code to Airline IDs
	 */
	private static HashMap<String, Integer> loadAirlines(String AIRLINES_FILE, Connection conn)
			throws FileNotFoundException, SQLException, IOException {
		/*
		 * Reading in the data from the airlines file the database. Each element is a
		 * list of strings, where the first element is the airline code and the second
		 * element is the airline name
		 */
		CSVReader reader_al = new CSVReader(new FileReader(AIRLINES_FILE));
		String[] nextLine_al;
		// Counter to be used for lookup when loading data into the array
		int i = 1;
		HashMap<String, Integer> airline_code_to_id = new HashMap<>();
		PreparedStatement prep = conn
				.prepareStatement("INSERT OR IGNORE INTO airlines (airline_code, airline_name) VALUES (?, ?)");
		while ((nextLine_al = reader_al.readNext()) != null) {
			prep.setString(1, nextLine_al[0]);
			prep.setString(2, nextLine_al[1]);
			prep.addBatch();
			// Adding to a HashMap for future reference
			airline_code_to_id.put(nextLine_al[0], i);
			i += 1;
		}
		conn.setAutoCommit(false);
		prep.executeBatch();
		conn.setAutoCommit(true);
		reader_al.close();
		return airline_code_to_id;
	}

	/*
	 * Method Description: Method that returns a formatted string for a time. Input
	 * - Unformatted time string. Returns - Formatted time string
	 */
	public static String returnCorrectTimeString(String str) throws ParseException {
		// choose a standard time format that will be used throughout the database
		DateFormat standardFormat = new SimpleDateFormat("HH:mm");
		standardFormat.setLenient(false);
		// Incorrect time formats that we want to fix
		DateFormat wrongDateFormat1 = new SimpleDateFormat("hh:mm a");
		wrongDateFormat1.setLenient(false);
		DateFormat wrongDateFormat2 = new SimpleDateFormat("KK:mm a");
		wrongDateFormat2.setLenient(false);
		DateFormat wrongDateFormat3 = new SimpleDateFormat("HH:mm");
		wrongDateFormat3.setLenient(false);
		try {
			/*
			 * Trying to parse the date using the correct format. If it works, then we
			 * return the string
			 */
			Date normalizedTime = wrongDateFormat1.parse(str);
			String normalizedTimeString = standardFormat.format(normalizedTime);
			return normalizedTimeString;
		} catch (ParseException e) {
			try {
				/*
				 * If an exception is thrown, we begin iterating through the rest of the
				 * possible formats to turn the string into the correct format
				 */
				Date normalizedTime = wrongDateFormat2.parse(str);
				String normalizedTimeString = standardFormat.format(normalizedTime);
				return normalizedTimeString;
			} catch (ParseException f) {
				try {
					Date normalizedTime = wrongDateFormat3.parse(str);
					String normalizedTimeString = standardFormat.format(normalizedTime);
					return normalizedTimeString;
				} catch (ParseException g) {
					try {
						// Testing if it is already in correct format
						Date normalizedTime = standardFormat.parse(str);
						String normalizedTimeString = standardFormat.format(normalizedTime);
						return normalizedTimeString;
					} catch (ParseException h) {
						// Throwing an error if we fail to fix the time
						throw h;

					}

				}

			}

		}

	}

	/*
	 * Method Description: Method that returns a formatted string for a date Input -
	 * An unformatted date string Returns - A formatted date string TESTED AND
	 * WORKING
	 */
	public static String returnCorrectDateString(String str) throws ParseException {
		// choose a standard date format that will be used throughout the database
		DateFormat standardFormat = new SimpleDateFormat("yyyy-MM-dd");
		standardFormat.setLenient(false);
		// All possible incorrect date formats
		DateFormat wrongDateFormat1 = new SimpleDateFormat("MM-dd-yyy");
		wrongDateFormat1.setLenient(false);
		DateFormat wrongDateFormat2 = new SimpleDateFormat("yyyy/MM/dd");
		wrongDateFormat2.setLenient(false);
		DateFormat wrongDateFormat3 = new SimpleDateFormat("MM/dd/yyyy");
		wrongDateFormat3.setLenient(false);
		DateFormat wrongDateFormat4 = new SimpleDateFormat("M/dd/yyyy");
		wrongDateFormat4.setLenient(false);
		DateFormat wrongDateFormat5 = new SimpleDateFormat("M/d/yyyy");
		wrongDateFormat5.setLenient(false);
		DateFormat wrongDateFormat6 = new SimpleDateFormat("MM/d/yyyy");
		wrongDateFormat6.setLenient(false);

		try { // if the parse method doesn't throw an exception, then the format matches the
				// date string. This case handles the case in which the parsed string already
				// matches the desired format
			Date normalizedDate = standardFormat.parse(str);
			// Converting to a string
			String normalizedDateString = standardFormat.format(normalizedDate);
			// Returning the normalized string
			return normalizedDateString;

		} catch (ParseException e) {
			// The sample format doesn't match the sample date string. Trying different
			// string types
			try {
				Date normalizedDate = wrongDateFormat1.parse(str);
				String normalizedDateString = standardFormat.format(normalizedDate);
				return normalizedDateString;

			} catch (ParseException f) {

				try {
					Date normalizedDate = wrongDateFormat2.parse(str);
					String normalizedDateString = standardFormat.format(normalizedDate);
					return normalizedDateString;

				} catch (ParseException g) {
					try {
						Date normalizedDate = wrongDateFormat3.parse(str);
						String normalizedDateString = standardFormat.format(normalizedDate);
						return normalizedDateString;
					} catch (ParseException h) {
						try {
							Date normalizedDate = wrongDateFormat4.parse(str);
							String normalizedDateString = standardFormat.format(normalizedDate);
							return normalizedDateString;

						} catch (ParseException i) {
							try {
								Date normalizedDate = wrongDateFormat5.parse(str);
								String normalizedDateString = standardFormat.format(normalizedDate);
								return normalizedDateString;

							} catch (ParseException j) {
							}
							try {
								// Last try to fix the date
								Date normalizedDate = wrongDateFormat6.parse(str);
								// Converting to a string
								String normalizedDateString = standardFormat.format(normalizedDate);
								return normalizedDateString;

							} catch (ParseException k) {
								// Throwing the exception if nothing worked
								throw k;
							}

						}

					}
				}

			}
		}

	}

}
