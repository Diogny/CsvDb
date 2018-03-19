using CsvDb;
using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Configuration;

namespace ConsoleApp
{
	public class Program
	{

		public static IConfiguration Configuration => new ConfigurationBuilder()
				.SetBasePath(System.IO.Directory.GetCurrentDirectory())
				.AddJsonFile("config.json")
				.Build();

		static void Main(string[] args)
		{
			var handleWait = false;

			//Generators();
			//TestHeaders();

			//SqlQueryExecuteTests();
			//SqlQueryFinalParseTests();
			handleWait = SqlQueryExecute();

			if (!handleWait)
			{
				Console.WriteLine("\r\nPress enter to finish!");
				Console.ReadLine();
			}
		}

		static CsvDb.CsvDb OpenDatabase(string dbName = null, bool logTimes = true)
		{
			var section = Program.Configuration.GetSection("Data");
			var basePath = section["BasePath"];

			if (string.IsNullOrWhiteSpace(dbName))
			{
				dbName = "data\\";
			}
			string rootPath = $"{basePath}{dbName}"
			//$@"{basePath}data\"
			//$@"{basePath}data-light\"
			//$@"{basePath}data-extra-light\"
			;
			if (!rootPath.EndsWith('\\'))
			{
				rootPath += "\\";
			}

			var dif = new TimeDifference();
			if (logTimes)
			{
				dif.Start = DateTime.Now;
			}

			CsvDb.CsvDb db = null;
			try
			{
				db = new CsvDb.CsvDb(rootPath);

				if (logTimes)
				{
					dif.End = DateTime.Now;
					Console.WriteLine($" opened on: {dif}");
				}
			}
			catch (Exception ex)
			{
				db = null;
				Console.WriteLine($"error: {ex.Message}");
			}
			return db;
		}

		static void Generators()
		{
			//update CreateDatabase() rootPath to match with [name].zip file  data\ with bus_data.zip

			var db = OpenDatabase();

			var gen = new CsvDbGenerator(
				db,
				//@"{basePath}bus_data.zip",
				//@"{basePath}bus_data-light.zip",
				@"{basePath}bus_data-extra-light.zip",

				//comment this when uncommented: gen.GenerateTxtData(); for a clean output
				removeAll: false
			);
			//gen.GenerateTxtData();

			//gen.CompilePagers();

			gen.CompileIndexes();

			gen.GenerateTreeStructure();
		}

		static bool SqlQueryExecute()
		{
			var availableDatabases = new string[]
			{
				"data",
				"data-light",
				"data-extra-light"
			};

			//SELECT * FROM agency

			var dif = new TimeDifference();

			//var db = CreateDatabase();
			CsvDb.CsvDb db = null;

			//Action displayHelp = () =>
			void displayHelp()
			{
				Console.WriteLine("");
				Console.WriteLine("Menu");
				Console.WriteLine(" Shows (h)elp");
				Console.WriteLine(" (D)isplay database(s)");
				Console.WriteLine(" (M)ount database");
				Console.WriteLine(" (U)nmount database");
				Console.WriteLine(" (S)earch Database");
				Console.WriteLine(" Display (t)ables");
				Console.WriteLine(" Display (c)olumn");
				Console.WriteLine(" Clea(r)");
				Console.WriteLine(" (Q)uit");
			}

			//Console.TreatControlCAsInput = true;
			bool end = false;
			//displayHelp();
			while (!end)
			{
				Console.Write(">");

				while (Console.KeyAvailable == false)
					Thread.Sleep(250); // Loop until input is entered.

				ConsoleKeyInfo key = Console.ReadKey();
				Console.WriteLine();
				switch (key.Key)
				{
					case ConsoleKey.M:
						Console.Write("database name >");
						var dbName = Console.ReadLine();
						if ((db = OpenDatabase(dbName: dbName, logTimes: true)) != null)
						{
							Console.WriteLine($"\r\nUsing database: {db.Name}");
						}
						break;
					case ConsoleKey.U:
						if (db == null)
						{
							Console.WriteLine($" no database to unmount");
						}
						else
						{
							Console.WriteLine($" unmounting database [{db.Name}]");
							db = null;
						}
						break;
					case ConsoleKey.D:
						var prefix = "\r\n   -";
						var txt = String.Join(prefix, availableDatabases);
						if (String.IsNullOrWhiteSpace(txt))
						{
							txt = $"{prefix}there is no available database";
						}
						else
						{
							txt = $"{prefix}{txt}";
						}
						Console.WriteLine($" database(s){txt}");
						break;
					case ConsoleKey.H:
						displayHelp();
						break;
					case ConsoleKey.T:
						//display tables
						if (db == null)
						{
							Console.WriteLine(" there's no database in use");
							break;
						}
						foreach (var t in db.Tables)
						{
							Console.WriteLine($" ({t.Columns.Count}) {t.Name}:{t.Type}, multikey: {t.Multikey} rows: {t.Rows}");
						}
						break;
					case ConsoleKey.C:
						//if ((key.Modifiers & ConsoleModifiers.Control) != 0)
						//{
						//	// Console.Write("CTL+");
						//	end = true;
						//}
						//else
						{
							if (db == null)
							{
								Console.WriteLine(" there's no database in use");
								break;
							}
							//show column structure
							Console.Write("table name >");
							var tableName = Console.ReadLine();
							var table = db.Table(tableName);
							if (table == null)
							{
								Console.WriteLine($" invalid table [{tableName}]");
							}
							else
							{
								foreach (var col in table.Columns)
								{
									Console.WriteLine($" [{table.Name}].{col.Name}:{col.Type}");
									Console.WriteLine($"  key: {col.Key}, indexed: {col.Indexed}, unique: {col.Unique}, pages: {col.PageCount}");
								}
							}
						}
						break;
					case ConsoleKey.S:
						try
						{
							if (db == null)
							{
								Console.WriteLine(" there's no database in use");
								break;
							}

							Console.Write(" query >");
							var query = Console.In.ReadLine();
							Console.WriteLine();
							//Console.WriteLine($" processing: {query}");

							var parser = new CsvDbQueryParser();
							dif.Start = DateTime.Now;
							var dbQuery = parser.Parse(db, query);
							dif.End = DateTime.Now;
							Console.WriteLine($" query parsed on: {dif}");

							//to calculate times
							dif.Start = DateTime.Now;

							var visualizer = new CsvDbVisualizer(dbQuery);
							var rows = visualizer.Execute().ToList();

							dif.End = DateTime.Now;
							Console.WriteLine($" {rows.Count} row(s) retrieved on: {dif}");

							//header
							dif.Start = DateTime.Now;

							var header = String.Join("|", dbQuery.Columns.Header);
							Console.WriteLine($"\r\n{header}");
							Console.WriteLine($"{new string('-', header.Length)}");

							foreach (var record in visualizer.Execute())
							{
								Console.WriteLine($"{String.Join(",", record)}");
							}

							dif.End = DateTime.Now;
							Console.WriteLine($"\r\n displayed on: {dif}");
						}
						catch (Exception ex)
						{
							Console.WriteLine($"error: {ex.Message}");
						}
						break;
					case ConsoleKey.Q:
						end = true;
						break;
					case ConsoleKey.R:
						Console.Clear();
						break;
					default:
						Console.WriteLine(" -invalid option, press [h] for help");
						break;
				}
			}
			return true;
		}

		static void SqlQueryExecuteTests()
		{
			var dif = new TimeDifference();
			dif.Start = DateTime.Now;

			var db = OpenDatabase();

			dif.End = DateTime.Now;

			Console.WriteLine($"\r\nDatabase: {db.Name}");
			Console.WriteLine($">created on: {dif}");

			var query = //"SELECT * FROM routes WHERE route_id >= (Int32)5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id = 180"
									//		180,NJB,74,,3,,
				"SELECT * FROM trips WHERE route_id = 204 AND service_id = 5 AND trip_id = 52325"
				//		204,5,52325,"810 WOODBRIDGE CENTER-Exact Fare",1,810MX003,6369
				;

			Console.WriteLine($"\r\nIn query: {query}");

			dif.Start = DateTime.Now;

			var parser = new CsvDbQueryParser();
			var dbQuery = parser.Parse(db, query); // old one: CsvDb.CsvDbQuery.Parse(db, query);

			dif.End = DateTime.Now;
			Console.WriteLine($">parsed on: {dif}");

			var outQuery = dbQuery.ToString();

			Console.WriteLine($"\r\nOut query: {outQuery}\r\n");

			//to calculate times
			dif.Start = DateTime.Now;

			var visualizer = new CsvDbVisualizer(dbQuery);
			var rows = visualizer.Execute().ToList();

			dif.End = DateTime.Now;
			Console.WriteLine($">{rows.Count} row(s) retrieved on: {dif}");

			//header
			dif.Start = DateTime.Now;

			var header = String.Join("|", dbQuery.Columns.Header);
			Console.WriteLine($"\r\n{header}");
			Console.WriteLine($"{new string('-', header.Length)}");

			foreach (var record in visualizer.Execute())
			{
				Console.WriteLine($"{String.Join(",", record)}");
			}

			dif.End = DateTime.Now;
			Console.WriteLine($"\r\n>row(s) displayed on: {dif}");

		}

		static void SqlQueryFinalParseTests()
		{
			var db = OpenDatabase();

			Console.WriteLine($"\r\nDatabase: {db.Name}");

			var queryCollection = new string[]
			{
				//errors
				"",
				"SELECT",
				"SELECT *",
				"SELECT * FROM",
				"SELECT * FROM none",
				"SELECT * FROM routes WHERE ",
				"SELECT * FROM routes WHERE SKIP 2 ",
				"SELECT * FROM routes SKIP ",
				"SELECT * FROM routes WHERE route_id ",
				"SELECT * FROM routes WHERE route_id > ",
				"SELECT * FROM routes WHERE > 5 SKIP 2",
				"SELECT * FROM routes WHERE route_id > SKIP 2",
				//this one is because cookoo_i is not a table and tries to convert to number
				"SELECT * FROM routes WHERE cookoo_i > 4 SKIP 2",

				//
				"SELECT * FROM routes",
				"SELECT * FROM routes SKIP 2",
				"SELECT * FROM routes SKIP 2 LIMIT 5",
				"SELECT * FROM routes WHERE route_id >= 5",
				"SELECT * FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\"",
				"SELECT * FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2",
				"SELECT * FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5",
				//
				"SELECT route_id, agency_id,route_type FROM routes",
				"SELECT route_id, agency_id,route_type FROM routes SKIP 2",
				"SELECT route_id, agency_id,route_type FROM routes SKIP 2 LIMIT 2",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\"",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 2"
			};

			foreach (var query in queryCollection)
			{
				try
				{
					Console.WriteLine($"\r\n{new string('-', Console.WindowHeight - 1)}");

					Console.WriteLine($"\r\n>{query}");

					var parser = new CsvDbQueryParser();
					var queryDb = parser.Parse(db, query);

					var outQuery = queryDb.ToString();

					Console.WriteLine($"\r\n>{outQuery}");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"exception: {ex.Message}");
				}
			}
		}

		static void TestHeaders()
		{
			var db = OpenDatabase();

			//this is just for testing
			var vis = new Visualizer(db);

			//vis.TestIndexTrees();
			vis.TestIndexTreeShape();

			//	vis.TestOverwrite();

			//vis.TestIndexItems();
		}

	}

}
