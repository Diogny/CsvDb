using CsvDb;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsoleApp
{
	class Program
	{
		static void Main(string[] args)
		{
			//Generators();
			//TestHeaders();

			//SqlQueryTests();
			//TestSearch();
			SqlQueryExecuteTests();

			Console.WriteLine("\r\nPress enter to finish!");
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

			var db = new CsvDb.CsvDb(rootPath);

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

		static void SqlQueryExecuteTests()
		{
			string rootPath =
		@"C:\Users\Diogny\Desktop\NJTransit\data\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);

			Console.WriteLine($"\r\nDatabase: {db.Name}");

			var query = //"SELECT * FROM routes WHERE route_id >= (Int32)5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id = 180"
									//		180,NJB,74,,3,,
				"SELECT * FROM trips WHERE route_id = 204 AND service_id = 5 AND trip_id = 52325"
				//		204,5,52325,"810 WOODBRIDGE CENTER-Exact Fare",1,810MX003,6369
				;

			Console.WriteLine($"\r\nIn query: {query}");

			var dbQuery = CsvDb.CsvDbQuery.Parse(db, query);

			var outQuery = dbQuery.ToString();

			Console.WriteLine($"\r\nOut query: {outQuery}");

			var visualizer = new CsvDbVisualizer(dbQuery);
			foreach (var record in visualizer.Execute())
			{
				Console.WriteLine($"{String.Join(",", record)}");
			}

		}

		static void SqlQueryTests()
		{
			string rootPath =
		//@"C:\Users\Diogny\Desktop\NJTransit\data\";
		//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
		@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);

			Console.WriteLine($"\r\nDatabase: {db.Name}");

			var query = //"SELECT * FROM routes WHERE route_id >= (Int32)5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
				//"select * from routes where"
				//"select * from routes"
				//"select * from"
				//"select *"
				//"select"
				//""
				;
			
			Console.WriteLine($"\r\nIn query: {query}");

			var dbQuery = CsvDb.CsvDbQuery.Parse(db, query);

			var outQuery = dbQuery.ToString();
			
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
			var tests = new List<object[]>()
			{
				new object[] { "agency", "agency_id", (String)"NJB" },
				//		NJB,NJ TRANSIT BUS,http://www.njtransit.com/,America/New_York,en,
				new object[] { "stops", "stop_id", (Int32)307 },
				//		307,10863,"SHORE RD AT MEYRAN AVE",,39.324568,-74.587541,0
				new object[] { "routes", "route_id", (Int32)180 },
				//	offset: 1320,		180|3073
				//		180,NJB,74,,3,,
				new object[] { "calendar_dates", "date", (Int32)20180422 },
				//		2,20180422,1
				//		8,20180422,1
				new object[]  { "trips", "trip_id", (Int32)61584 }
				//	offset: 494480
				//		253,2,61584,GO28 NEWARK AIRPORT NORTH AREA & TERMINALS-Exact Fare,0,258OG013,7334
			};
			foreach (var args in tests.Select(item => new
			{
				table = (String)item[0],
				column = (String)item[1],
				key = item[2]
			}))
			{
				Console.WriteLine($"\r\nTest for: {String.Join(",", args)}");

				try
				{
					var res = r.Find(args.table, args.column, "=", args.key);

					Console.WriteLine("Output>");
					foreach (var col in res)
					{
						Console.WriteLine($"{String.Join(",", col)}");
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
			}
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
		}

	}

}
