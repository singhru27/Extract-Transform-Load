package edu.brown.cs.cs127.etl.tester;

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import au.com.bytecode.opencsv.CSVReader;

public class ETLTATester extends TestSuite
{
	public static final String PUB_PATH = "/course/cs127/pub/etl/tests";

	public static Test suite()
	{
		TestSuite suite = new TestSuite("ETL Tests");
		suite.addTest(new ETLTATester());
		return suite;
	}

	public ETLTATester()
	{
		super();

		// Create one test for each set of files we have
		File[] files = new File(PUB_PATH).listFiles();
		Arrays.sort(files);
		for (File curTestDir : files)
		{
			addTest(new ETLTest(curTestDir));
		}
	}

	private static class ETLTest extends TestCase
	{
		private File testDir;

		public ETLTest(File testDir)
		{
			this.testDir = testDir;
			setName(testDir.getAbsolutePath());
		}

		protected void runTest() throws Throwable
		{
			CSVReader solutionReader = new CSVReader(new FileReader(String.format("%s/output", this.testDir.getAbsolutePath())));
			List<String[]> solEntries = solutionReader.readAll();

			List<String> command = this.getCommand();
			ProcessBuilder pb = new ProcessBuilder(this.cleanCommand(command));
			pb.redirectErrorStream(true);
			Process p = pb.start();

			StringBuilder result = new StringBuilder();
			Scanner s = new Scanner(new InputStreamReader(p.getInputStream()));
			while (s.hasNextLine())
			{
				result.append(s.nextLine());
				result.append("\n");
			}

			// Print out some debug information
			System.out.print("Input: ");
			for (String curArg : command)
				System.out.print(curArg + " ");
			System.out.println();
			System.out.println("Output:\n" + result.toString());

			CSVReader resultReader = new CSVReader(new StringReader(result.toString()));
			List<String[]> resultEntries = resultReader.readAll();

			Assert.assertEquals("We don't have the same number of rows", solEntries.size(), resultEntries.size());

			if (solEntries.size() > 0)
				Assert.assertEquals("We don't have the same number of columns", solEntries.get(0).length, resultEntries.get(0).length);

			for (int r = 0; r < solEntries.size(); ++r)
			{
				for (int c = 0; c < solEntries.get(0).length; ++c)
				{
					Assert.assertEquals("Found a mismatch on row " + r + ", column " + c, solEntries.get(r)[c], resultEntries.get(r)[c]);
				}
			}
		}

		private List<String> getCommand() throws Throwable
		{
			List<String> result = new ArrayList<String>();
			result.add("./query");
			result.add("./data.db");

			String temp = "";
			Scanner s = new Scanner(new File(String.format("%s/input", this.testDir.getAbsolutePath())));
			while (s.hasNextLine())
			{
				temp += s.nextLine() + "\n";
			}

			result.addAll(Arrays.asList(temp.trim().split("\n")));

			return result;
		}

		private List<String> cleanCommand(List<String> command) throws Throwable
		{
			List<String> result = new ArrayList<String>();
			for (String arg : command)
			{
				if (arg.charAt(0) == '\"' && arg.charAt(arg.length() - 1) == '\"')
				{
					result.add(arg.substring(1, arg.length() - 1));
				}
				else
				{
					result.add(arg);
				}
			}

			return result;
		}
	}
}
