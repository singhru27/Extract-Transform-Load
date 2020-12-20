package edu.brown.cs.cs127.etl.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EtlQuery {
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception {
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}

	public ResultSet queryA(String[] args) throws SQLException {

		PreparedStatement stat = conn.prepareStatement("SELECT COUNT(?) FROM airports");
		stat.setString(1, "airport_code");
		return stat.executeQuery();
	}

	public ResultSet queryB(String[] args) throws SQLException {

		PreparedStatement stat = conn.prepareStatement("SELECT COUNT(?) FROM airlines");
		stat.setString(1, "airline_code");
		return stat.executeQuery();
	}

	public ResultSet queryC(String[] args) throws SQLException {

		PreparedStatement stat = conn.prepareStatement("SELECT COUNT(?) FROM flights");
		stat.setString(1, "*");
		return stat.executeQuery();
	}

	public ResultSet query0(String[] args) throws SQLException {

		PreparedStatement stat = conn.prepareStatement(
				"SELECT airport_name, city, state FROM airports DESC WHERE state LIKE 'Alaska' ORDER BY city DESC LIMIT 3");
		return stat.executeQuery();
	}

	public ResultSet query1(String[] args) throws SQLException {
		String airline_name = args[0];
		PreparedStatement stat = conn.prepareStatement(
				"SELECT airline_name, MIN(flight_num) FROM flights join airlines on flights.airline_id = airlines.airline_id WHERE airlines.airline_name = ?");
		stat.setString(1, airline_name);
		return stat.executeQuery();

	}

	public ResultSet query2(String[] args) throws SQLException {
		String airline_name = args[0];
		PreparedStatement stat = conn.prepareStatement(
				"SELECT COUNT(*) FROM flights join airlines on flights.airline_id = airlines.airline_id WHERE airlines.airline_name = ? AND flights.cancelled = 1");
		stat.setString(1, airline_name);
		return stat.executeQuery();

	}

	public ResultSet query3(String[] args) throws SQLException {
		PreparedStatement stat = conn.prepareStatement(
				"WITH high_traffic_airlines(airline_id) AS (SELECT flights.airline_id FROM flights GROUP BY flights.airline_id HAVING COUNT(flights.flight_id) > 10000), "
						+ "high_traffic_airports (origin_airport_id, flight_count) AS (SELECT flights.origin_airport_id, COUNT(flights.flight_id) FROM flights WHERE flights.airline_id IN high_traffic_airlines GROUP BY flights.origin_airport_id ORDER BY COUNT(flights.flight_id) DESC LIMIT 5)"
						+ "SELECT airports.airport_name, high_traffic_airports.flight_count FROM airports join high_traffic_airports on airports.airport_id = high_traffic_airports.origin_airport_id");
		return stat.executeQuery();

	}

	public ResultSet query4(String[] args) throws SQLException {
		PreparedStatement stat = conn.prepareStatement(
				"WITH carrier_count(name, num) AS (SELECT 'Carrier Delay', count(*) FROM flights WHERE carrier_delay>0),"
						+ "weather_delay_count(name, num) AS (SELECT 'Weather Delay', count(*) FROM flights WHERE weather_delay>0),"
						+ "at_delay_count(name, num) AS (SELECT 'Air Traffic Delay', count(*) FROM flights WHERE air_traffic_delay>0),"
						+ "sec_delay_count(name, num) AS (SELECT 'Security Delay', count(*) FROM flights WHERE security_delay>0)"
						+ ", delay_count (name, num) AS (SELECT * FROM carrier_count UNION ALL SELECT * FROM weather_delay_count UNION ALL SELECT * FROM at_delay_count UNION ALL SELECT * FROM sec_delay_count)"
						+ "SELECT * FROM delay_count ORDER BY num DESC");
		return stat.executeQuery();

	}

	public ResultSet query5(String[] args) throws SQLException, ParseException {
		String date_unformatted = args[2] + "-" + args[0] + "-" + args[1];
		String date_formatted = returnCorrectDateString(date_unformatted);

		PreparedStatement stat = conn.prepareStatement(
				"WITH matching_airlines(airline_id, num_flights) AS (SELECT airline_id, COUNT(flight_id) FROM flights WHERE departure_date LIKE (?) GROUP BY airline_id),"
						+ "matching_ids (airline_id) AS (SELECT airline_id FROM matching_airlines),"
						+ "non_matching_airlines (airline_id, num_flights) AS (SELECT airline_id, 0 FROM airlines WHERE airline_id NOT IN matching_ids),"
						+ "total_airlines AS (SELECT * FROM matching_airlines UNION ALL SELECT * FROM non_matching_airlines)"
						+ "SELECT airline_name, num_flights FROM total_airlines JOIN airlines ON total_airlines.airline_id = airlines.airline_id ORDER BY num_flights DESC, airline_name");
		stat.setString(1, date_formatted);
		return stat.executeQuery();
	}

	public ResultSet query6(String[] args) throws SQLException, ParseException {
		String date_unformatted = args[2] + "-" + args[0] + "-" + args[1];
		String date_formatted = returnCorrectDateString(date_unformatted);

		String base_query = "WITH matching_airport_id(airport_id) AS (";

		for (int i = 0; i < args.length; i++) {

			if (i > 2) {
				if (i == 3) {
					base_query = base_query + "SELECT airport_ID FROM airports WHERE airports.airport_name = " + "'"
							+ args[i] + "' ";
				} else {
					base_query = base_query + "UNION SELECT airport_ID FROM airports WHERE airports.airport_name = "
							+ "'" + args[i] + "' ";
				}
			}
		}
		base_query = base_query + ")";
		PreparedStatement stat = conn.prepareStatement(base_query
				+ ", depart_count (airport_id, depart_num) AS (SELECT matching_airport_id.airport_id, COUNT(*) FROM matching_airport_id JOIN flights ON matching_airport_id.airport_id = flights.origin_airport_id WHERE flights.departure_date LIKE (?) GROUP BY matching_airport_id.airport_id )"
				+ ", arrival_count (airport_id, arrival_num) AS (SELECT matching_airport_id.airport_id, COUNT(*) FROM matching_airport_id JOIN flights ON matching_airport_id.airport_id = flights.dest_airport_id WHERE flights.arrival_date LIKE (?) GROUP BY matching_airport_id.airport_id )"
				+ ", joined_depart (airport_id, depart_num) AS (SELECT matching_airport_id.airport_id, depart_count.depart_num FROM matching_airport_id LEFT JOIN depart_count ON matching_airport_id.airport_id = depart_count.airport_id)"
				+ ", joined_arrive (airport_id, depart_num, arrival_num) AS (SELECT joined_depart.airport_id, joined_depart.depart_num, arrival_count.arrival_num FROM joined_depart LEFT JOIN arrival_count ON joined_depart.airport_id = arrival_count.airport_id)"
				+ " SELECT airports.airport_name, joined_arrive.depart_num, joined_arrive.arrival_num FROM joined_arrive JOIN airports on joined_arrive.airport_id = airports.airport_id");

		stat.setString(1, date_formatted);
		stat.setString(2, date_formatted);

		return stat.executeQuery();
	}

	public ResultSet query7(String[] args) throws SQLException, ParseException {
		String airline_name = args[0];
		int flight_number = Integer.valueOf(args[1]);
		String start_date = returnCorrectDateString(args[2]);
		String end_date = returnCorrectDateString(args[3]);

		PreparedStatement stat = conn.prepareStatement(""
				+ "WITH id(airline_id) AS (SELECT airlines.airline_id FROM airlines WHERE airlines.airline_name = (?))"
				+ ", filtered_flights AS (SELECT * FROM flights JOIN id ON flights.airline_id = id.airline_id WHERE flights.flight_num = (?) AND flights.departure_date >= (?) AND flights.departure_date <= (?))"
				+ ", total_count (airline_id, total) AS (SELECT filtered_flights.airline_id, COUNT (*) FROM filtered_flights GROUP BY filtered_flights.airline_id)"
				+ ", cancelled_count (airline_id, cancelled) AS (SELECT filtered_flights.airline_id, COUNT(*) FROM filtered_flights WHERE filtered_flights.cancelled > 0)"
				+ ", early_depart (airline_id, early_depart) AS (SELECT filtered_flights.airline_id, COUNT(*) FROM filtered_flights WHERE filtered_flights.cancelled = 0 AND filtered_flights.departure_diff <=0) "
				+ ", late_depart (airline_id, late_depart) AS (SELECT filtered_flights.airline_id, COUNT(*) FROM filtered_flights WHERE filtered_flights.cancelled = 0 AND filtered_flights.departure_diff > 0)"
				+ ", early_arrive (airline_id, early_arrive) AS (SELECT filtered_flights.airline_id, COUNT(*) FROM filtered_flights WHERE filtered_flights.cancelled = 0 AND filtered_flights.arrival_diff <=0)"
				+ ", late_arrive (airline_id, late_arrive) AS (SELECT filtered_flights.airline_id, COUNT(*) FROM filtered_flights WHERE filtered_flights.cancelled = 0 AND filtered_flights.arrival_diff > 0)"
				+ " SELECT total, cancelled, early_depart, late_depart, early_arrive, late_arrive FROM total_count NATURAL JOIN cancelled_count NATURAL JOIN early_depart NATURAL JOIN late_depart NATURAL JOIN early_arrive NATURAL JOIN late_arrive"
				);
		
		stat.setString(1, airline_name);
		stat.setInt(2, flight_number);
		stat.setString(3, start_date);
		stat.setString(4, end_date);

		return stat.executeQuery();
	}
	
	public ResultSet query8(String[] args) throws SQLException, ParseException {
		String departure_city = args[0];
		String departure_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String earliest_datetime = returnCorrectDateString(args[4])+ " 00:00";
		String latest_datetime = returnCorrectDateString(args[4]) + " 23:59";
		
		PreparedStatement stat = conn.prepareStatement(""
				+ "WITH compressed_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime) AS (SELECT airline_id, flight_num, origin_airport_id, strftime('%H:%M', departure_time, departure_diff||' minutes'),datetime(departure_date||departure_time, departure_diff||' minutes'), dest_airport_id, strftime('%H:%M', arrival_time, arrival_diff||' minutes'), datetime(arrival_date||arrival_time, arrival_diff||' minutes') FROM flights WHERE cancelled=0)"
				+ ", total_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime, duration) AS (SELECT *, (strftime('%s',arrival_datetime) - strftime('%s', departure_datetime))/60 FROM compressed_flights)"
				+ ", date_restricted_flights (airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time, duration) AS (SELECT airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time, duration FROM total_flights WHERE departure_datetime >= (?) AND arrival_datetime <= (?))"
				+ ", filtered_origin_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"
				+ ", filtered_arrival_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"
				+ ", filtered_origin_flights (airline_id, flight_num, airport_code, departure_time, arrival_airport_id, arrival_time, duration) AS (SELECT airline_id, flight_num, airport_code, departure_time, arrival_airport_id, arrival_time, duration FROM date_restricted_flights JOIN filtered_origin_airports ON date_restricted_flights.origin_airport_id = filtered_origin_airports.airport_id)"
				+ ", filtered_arrival_flights (airline_id, flight_num, origin_airport_code, departure_time, arrival_airport_code, arrival_time, duration) AS (SELECT airline_id, flight_num, filtered_origin_flights.airport_code, departure_time, filtered_arrival_airports.airport_code, arrival_time, duration FROM filtered_origin_flights JOIN filtered_arrival_airports ON filtered_origin_flights.arrival_airport_id = filtered_arrival_airports.airport_id)"
				+ ", final_result (airline_code, flight_num, origin_airport_code, departure_time, arrival_airport_code, arrival_time, duration) AS (SELECT airline_code, flight_num, origin_airport_code, departure_time, arrival_airport_code, arrival_time, duration FROM filtered_arrival_flights JOIN airlines ON filtered_arrival_flights.airline_id = airlines.airline_id)"
				+ " SELECT * FROM final_result ORDER BY duration, airline_code"
		);
		stat.setString(1, earliest_datetime);
		stat.setString(2, latest_datetime);
		stat.setString(3, departure_city);
		stat.setString(4, departure_state);
		stat.setString(5, arrival_city);
		stat.setString(6, arrival_state);

		return stat.executeQuery();
	}
	

	
	public ResultSet query9(String[] args) throws SQLException, ParseException {
		String departure_city = args[0];
		String departure_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String earliest_datetime = returnCorrectDateString(args[4])+ " 00:00";
		String latest_datetime = returnCorrectDateString(args[4]) + " 23:59";
		
		PreparedStatement stat = conn.prepareStatement(""
				+ "WITH compressed_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime) AS (SELECT airline_id, flight_num, origin_airport_id, strftime('%H:%M',departure_time, departure_diff||' minutes'),datetime(departure_date||departure_time, departure_diff||' minutes'), dest_airport_id, strftime('%H:%M', arrival_time, arrival_diff||' minutes'), datetime(arrival_date||arrival_time, arrival_diff||' minutes') FROM flights WHERE cancelled=0)"
				+ ", total_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime, duration) AS (SELECT *, (strftime('%s',arrival_datetime) - strftime('%s', departure_datetime))/60 FROM compressed_flights)"
				+ ", date_restricted_flights (airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time) AS (SELECT airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time FROM total_flights WHERE departure_datetime >= (?) AND arrival_datetime <= (?))"
				+ ", filtered_origin_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"
				+ ", filtered_arrival_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"				
				+ ", filtered_origin_airport_id (airport_id) AS (SELECT airport_id FROM filtered_origin_airports)"
				+ ", filtered_arrival_airport_id (airport_id) AS (SELECT airport_id FROM filtered_arrival_airports)"
				+ ", leg1flights (airline_id, flight_num, origin_airport_code, departure_time, arrival_airport_id, arrival_time) AS (SELECT d.airline_id, d.flight_num, airport_code, departure_time, arrival_airport_id, arrival_time FROM date_restricted_flights AS d JOIN filtered_origin_airports AS f ON d.origin_airport_id = f.airport_id WHERE d.arrival_airport_id NOT IN filtered_origin_airport_id AND d.arrival_airport_id NOT IN filtered_arrival_airport_id)"
				+ ", leg1flights2 (airline_id, flight_num, origin_airport_code, departure_time, arrival_airport_id, arrival_airport_code, arrival_time) AS (SELECT l.airline_id, l.flight_num, l.origin_airport_code, l.departure_time, l.arrival_airport_id, a.airport_code, l.arrival_time FROM leg1flights as l JOIN airports AS a ON l.arrival_airport_id = a.airport_id)"
				+ ", leg2flights (airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_code, arrival_time) AS (SELECT d.airline_id, d.flight_num, d.origin_airport_id, d.departure_time, f.airport_code, d.arrival_time FROM date_restricted_flights AS d JOIN filtered_arrival_airports AS f ON d.arrival_airport_id = f.airport_id WHERE d.origin_airport_id NOT IN filtered_origin_airport_id AND d.origin_airport_id NOT IN filtered_arrival_airport_id) "
				+ ", leg2flights2 (airline_id, flight_num, origin_airport_id, origin_airport_code, departure_time, arrival_airport_code, arrival_time) AS (SELECT l2.airline_id, l2.flight_num, l2.origin_airport_id, a.airport_code, departure_time, arrival_airport_code, arrival_time FROM leg2flights AS l2 JOIN airports AS a ON l2.origin_airport_id = a.airport_id) "
				+ ", concatenated_flights(l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_id, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_id, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time)  AS (SELECT * FROM leg1flights2 AS l JOIN leg2flights2 AS l2 ON l.arrival_airport_id = l2.origin_airport_id)"
				+ ", timed_flights(l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_id, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_id, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time) AS (SELECT * FROM concatenated_flights AS c WHERE c.l2departure_time > c.l1arrival_time)"
				+ ", scrubbed_flights(l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, (strftime('%s',l2arrival_time) - strftime('%s', l1departure_time))/60 FROM timed_flights)"
				+ ", l1airline_code (l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration FROM scrubbed_flights JOIN airlines ON scrubbed_flights.l1airline_id = airlines.airline_id)"
				+ ", l2airline_code (l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l2airline_code, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, airline_code, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration FROM l1airline_code JOIN airlines ON l1airline_code.l2airline_id = airlines.airline_id)"
				+ " SELECT * FROM l2airline_code ORDER BY duration, l1airline_code, l2airline_code, l1departure_time"
		);
		stat.setString(1, earliest_datetime);
		stat.setString(2, latest_datetime);
		stat.setString(3, departure_city);
		stat.setString(4, departure_state);
		stat.setString(5, arrival_city);
		stat.setString(6, arrival_state);

		return stat.executeQuery();
	}

	
	public ResultSet query10(String[] args) throws SQLException, ParseException {
		String departure_city = args[0];
		String departure_state = args[1];
		String arrival_city = args[2];
		String arrival_state = args[3];
		String earliest_datetime = returnCorrectDateString(args[4])+ " 00:00";
		String latest_datetime = returnCorrectDateString(args[4]) + " 23:59";
		
		PreparedStatement stat = conn.prepareStatement(""
				+ "WITH compressed_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime) AS (SELECT airline_id, flight_num, origin_airport_id, strftime('%H:%M',departure_time, departure_diff||' minutes'),datetime(departure_date||departure_time, departure_diff||' minutes'), dest_airport_id, strftime('%H:%M', arrival_time, arrival_diff||' minutes'), datetime(arrival_date||arrival_time, arrival_diff||' minutes') FROM flights WHERE cancelled=0)"
				+ ", total_flights (airline_id, flight_num, origin_airport_id, departure_time, departure_datetime, arrival_airport_id, arrival_time, arrival_datetime, duration) AS (SELECT *, (strftime('%s',arrival_datetime) - strftime('%s', departure_datetime))/60 FROM compressed_flights)"
				+ ", date_restricted_flights (airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time) AS (SELECT airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_id, arrival_time FROM total_flights WHERE departure_datetime >= (?) AND arrival_datetime <= (?))"
				+ ", filtered_origin_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"
				+ ", filtered_arrival_airports (airport_id, airport_code) AS (SELECT airport_id, airport_code FROM airports WHERE city LIKE (?) AND state LIKE(?))"				
				+ ", filtered_origin_airport_id (airport_id) AS (SELECT airport_id FROM filtered_origin_airports)"
				+ ", filtered_arrival_airport_id (airport_id) AS (SELECT airport_id FROM filtered_arrival_airports)"
				+ ", leg1flights (airline_id, flight_num, origin_airport_code, departure_time, arrival_airport_id, arrival_time) AS (SELECT d.airline_id, d.flight_num, airport_code, departure_time, arrival_airport_id, arrival_time FROM date_restricted_flights AS d JOIN filtered_origin_airports AS f ON d.origin_airport_id = f.airport_id WHERE d.arrival_airport_id NOT IN filtered_origin_airport_id AND d.arrival_airport_id NOT IN filtered_arrival_airport_id)"
				+ ", leg1flights2 (airline_id, flight_num, origin_airport_code, departure_time, arrival_airport_id, arrival_airport_code, arrival_time) AS (SELECT l.airline_id, l.flight_num, l.origin_airport_code, l.departure_time, l.arrival_airport_id, a.airport_code, l.arrival_time FROM leg1flights as l JOIN airports AS a ON l.arrival_airport_id = a.airport_id)"
				+ ", leg2flights (airline_id, flight_num, origin_airport_id, departure_time, arrival_airport_code, arrival_time) AS (SELECT d.airline_id, d.flight_num, d.origin_airport_id, d.departure_time, f.airport_code, d.arrival_time FROM date_restricted_flights AS d JOIN filtered_arrival_airports AS f ON d.arrival_airport_id = f.airport_id WHERE d.origin_airport_id NOT IN filtered_origin_airport_id AND d.origin_airport_id NOT IN filtered_arrival_airport_id) "
				+ ", leg2flights2 (airline_id, flight_num, origin_airport_id, origin_airport_code, departure_time, arrival_airport_code, arrival_time) AS (SELECT l2.airline_id, l2.flight_num, l2.origin_airport_id, a.airport_code, departure_time, arrival_airport_code, arrival_time FROM leg2flights AS l2 JOIN airports AS a ON l2.origin_airport_id = a.airport_id) "
				+ ", leg3flights (airline_id, flight_num, origin_airport_id, origin_airport_code, departure_time, arrival_airport_id, arrival_time) AS (SELECT d.airline_id, d.flight_num, d.origin_airport_id, airport_code, d.departure_time, d.arrival_airport_id, d.arrival_time FROM date_restricted_flights AS d JOIN airports AS a ON d.origin_airport_id = a.airport_id WHERE d.arrival_airport_id NOT IN filtered_origin_airport_id AND d.arrival_airport_id NOT IN filtered_arrival_airport_id AND d.origin_airport_id NOT IN filtered_origin_airport_id AND d.origin_airport_id NOT IN filtered_arrival_airport_id)"
				+ ", leg3flights2 (airline_id, flight_num, origin_airport_id, origin_airport_code, departure_time, arrival_airport_id, arrival_airport_code, arrival_time) AS (SELECT d.airline_id, d.flight_num, d.origin_airport_id, d.origin_airport_code, d.departure_time, d.arrival_airport_id, a.airport_code, d.arrival_time FROM leg3flights AS d JOIN airports AS a ON d.arrival_airport_id = a.airport_id)"
				+ ", concatenated_flights (l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_id, l1arrival_airport_code, l1arrival_time, l3airline_id, l3flight_num, l3origin_airport_id, l3origin_airport_code, l3departure_time, l3arrival_airport_id, l3arrival_airport_code, l3arrival_time) AS (SELECT * FROM leg1flights2 AS l1 JOIN leg3flights2 AS l3 ON l1.arrival_airport_id = l3.origin_airport_id)"
				+ ", concatenated_flights2 (l1airline_id, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_id, l1arrival_airport_code, l1arrival_time, l3airline_id, l3flight_num, l3origin_airport_id, l3origin_airport_code, l3departure_time, l3arrival_airport_id, l3arrival_airport_code, l3arrival_time, l2airline_id, l2flight_num, l2origin_airport_id, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time) AS (SELECT * FROM concatenated_flights AS c JOIN leg2flights2 AS l2 ON c.l3arrival_airport_id = l2.origin_airport_id)"
				+ ", timed_flights AS (SELECT * FROM concatenated_flights2 WHERE l2departure_time > l3arrival_time AND l3departure_time > l1arrival_time)"
				+ ", scrubbed_flights AS (SELECT *,(strftime('%s',l2arrival_time) - strftime('%s', l1departure_time))/60 AS duration FROM timed_flights)"
				+ ", l1airline_code (l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l3airline_id, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l3airline_id, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration FROM scrubbed_flights AS s JOIN airlines AS a ON s.l1airline_id = a.airline_id)"
				+ ", l3airline_code (l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l3airline_code, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, airline_code, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, l2airline_id, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration FROM l1airline_code AS l1 JOIN airlines AS a ON l1.l3airline_id = a.airline_id)"
				+ ", l2airline_code (l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l3airline_code, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, l2airline_code, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration) AS (SELECT l1airline_code, l1flight_num, l1origin_airport_code, l1departure_time, l1arrival_airport_code, l1arrival_time, l3airline_code, l3flight_num, l3origin_airport_code, l3departure_time, l3arrival_airport_code, l3arrival_time, airline_code, l2flight_num, l2origin_airport_code, l2departure_time, l2arrival_airport_code, l2arrival_time, duration FROM l3airline_code AS l3 JOIN airlines AS a ON l3.l2airline_id = a.airline_id)"
				+ " SELECT * FROM l2airline_code ORDER BY duration, l1airline_code, l3airline_code, l2airline_code, l1departure_time"
		);
		stat.setString(1, earliest_datetime);
		stat.setString(2, latest_datetime);
		stat.setString(3, departure_city);
		stat.setString(4, departure_state);
		stat.setString(5, arrival_city);
		stat.setString(6, arrival_state);

		return stat.executeQuery();
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
