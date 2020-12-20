import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Testing {

	public static void main(String[] args) throws ParseException {

		String time = "11:29 PM";
		System.out.println(returnCorrectTimeString(time));
		
		String date = "05/20/2019";
		System.out.println(returnCorrectDateString(date));

	}
	/*
	 * Method Description: Method that returns a formatted string for a date Input -
	 * An unformatted date string Returns - A formatted date string
	 * TESTED AND WORKING
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


	/*
	 * Method Description: Method that returns a formatted string for a time
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

}
