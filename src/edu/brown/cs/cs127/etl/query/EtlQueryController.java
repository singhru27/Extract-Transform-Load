package edu.brown.cs.cs127.etl.query;

import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Arrays;

import au.com.bytecode.opencsv.CSVWriter;

public class EtlQueryController
{
	public static void main(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			System.err.println("This application requires at least two " +
								"parameters: the path to the SQLite database " +
								"and the pre-defined query to be run.");
			System.exit(1);
		}

		String DB_FILE = args[0];
		String QUERY = args[1];
		String[] PARAMS = Arrays.copyOfRange(args, 2, args.length);

		if (!QUERY.startsWith("query"))
			throw new NoSuchMethodException("Unknown method \"" + QUERY + "\"");

		EtlQuery query = new EtlQuery(DB_FILE);
		Method queryMethod = query.getClass().getDeclaredMethod(QUERY, PARAMS.getClass());
		ResultSet result = (ResultSet)queryMethod.invoke(query, new Object[]{PARAMS});

		if (result != null)
		{
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(System.out));
			writer.writeAll(result, false);
			writer.close();
		}
	}
}
