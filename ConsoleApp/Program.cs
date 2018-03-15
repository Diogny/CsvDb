using CsvDb;
using System;

namespace ConsoleApp
{
	class Program
	{
		//var dbStructPath = @"C:\Users\Diogny\Me\OneDrive\Projects\VS2017\CsvDb\CsvDbLib\";

		static void Main(string[] args)
		{

			//Generators();
			//TestHeaders();
			//SqlQueryTests();
			TestSearch();

			Console.WriteLine("Press enter to finish!");
			Console.ReadLine();
		}

		static void Generators()
		{
			string rootPath =
			//@"C:\Users\Diogny\Desktop\NJTransit\data\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
			@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";
			//
			//@"C:\Users\Diogny\Desktop\NJTransit\test\";

			var db = new CsvDb.CsvDb(
				////use this when start from zero
				//dbStructPath
				////use this just to compile pagers and indexes
				//   comment line: gen.GenerateTxtData();
				rootPath
			);

			var gen = new CsvDbGenerator(
				db,
				//@"C:\Users\Diogny\Desktop\NJTransit\bus_data.zip",
				//@"C:\Users\Diogny\Desktop\NJTransit\bus_data-light.zip",
				@"C:\Users\Diogny\Desktop\NJTransit\bus_data-extra-light.zip",

				//comment this when uncommented: gen.GenerateTxtData(); for a clean output
				removeAll: false
			);
			//gen.GenerateTxtData();

			//gen.CompilePagers();

			gen.CompileIndexes();

			gen.GenerateTreeStructure();
		}

		static void SqlQueryTests()
		{
			string rootPath =
		//@"C:\Users\Diogny\Desktop\NJTransit\data\";
		//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
		@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);

			Console.WriteLine($"\r\nDatabase: {db.Name}");

			var query = //"SELECT * FROM routes WHERE route_id >= 5 and agency_id <> \"NJB\" SKIP 2 LIMIT 5"
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 and agency_id <> \"NJB\" SKIP 2 LIMIT 5"
				//"select * from routes where"
				//"select * from routes"
				//"select * from"
				//"select *"
				//"select"
				//""
				;
			//select trip_headsign,block_id, trip_id,  route_id,service_id from trips where a> 7 and "p" = 8 skip 3 limit  10

			Console.WriteLine($"\r\nIn query: {query}");

			var dbQuery = CsvDb.CsvDbQuery.Parse(db, query);

			var outQuery = dbQuery.ToString();
			//"SELECT * FROM routes WHERE route_id = 5 and agency_id = \"NJB\""
			Console.WriteLine($"\r\nOut query: {outQuery}");
		}

		static void TestSearch()
		{
			string rootPath =
		@"C:\Users\Diogny\Desktop\NJTransit\data\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);

			var r = new CsvRecordReader(db);
			var res = r.Find("routes", "route_id", (Int32)180);
			/*
			 record = {string[7]}
			 */
		}

		static void TestHeaders()
		{
			string rootPath =
		@"C:\Users\Diogny\Desktop\NJTransit\data\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);

			//this is just for testing
			var vis = new Visualizer(db);

			//vis.TestIndexTrees();
			vis.TestIndexTreeShape();

			//	vis.TestOverwrite();

			//vis.TestIndexItems();
			vis.TestIndexReader("routes", "route_id", (Int32)180);
			//		"routes", "route_id", (Int32)180				//offset: 1320,		180|3073
			// only for \data\
			//		"trips", "trip_id", (Int32)61584				//offset: 494480		61584|3796690

			//with this value(s), we go the .csv to get the record(s)

		}

	}

}
