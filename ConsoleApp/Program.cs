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
			TestHeaders();

			Console.WriteLine("Press enter to finish!");
			Console.ReadLine();
		}

		static void Generators()
		{
			string rootPath =
		@"C:\Users\Diogny\Desktop\NJTransit\data\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
			//@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";
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
				@"C:\Users\Diogny\Desktop\NJTransit\bus_data.zip",
				//@"C:\Users\Diogny\Desktop\NJTransit\bus_data-light.zip",
				//@"C:\Users\Diogny\Desktop\NJTransit\bus_data-extra-light.zip",

				//comment this when uncommented: gen.GenerateTxtData(); for a clean output
				removeAll: false
			);
			//gen.GenerateTxtData();

			//gen.CompilePagers();

			gen.CompileIndexes();

			gen.GenerateTreeStructure();
		}

		static void TestHeaders()
		{
			string rootPath =
		//@"C:\Users\Diogny\Desktop\NJTransit\data\";
		//@"C:\Users\Diogny\Desktop\NJTransit\data-light\";
		@"C:\Users\Diogny\Desktop\NJTransit\data-extra-light\";

			var db = new CsvDb.CsvDb(rootPath);
			var vis = new Visualizer(db);

			//test.TestIndexTrees();
			vis.TestIndexTreeShape();

			//	test.TestOverwrite();

			//test.TestIndexItems();
			//test.TestIndexReader("routes", "route_id", (Int32)180);
			//		"routes", "route_id", (Int32)180				//offset: 1320,		180|3073
			// only for \data\
			//		"trips", "trip_id", (Int32)61584				//offset: 494480		61584|3796690
		}

	}

}
