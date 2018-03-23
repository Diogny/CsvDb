using CsvDb;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Csv.CMS.ConsApp
{
	class Program
	{
		public static IConfiguration Configuration { get; set; }

		static void Main(string[] args)
		{
			//https://docs.microsoft.com/en-us/aspnet/core/fundamentals/configuration/?tabs=basicconfiguration
			Configuration = new ConfigurationBuilder()
				.SetBasePath(System.IO.Directory.GetCurrentDirectory())
				.AddJsonFile("appsettings.json") //"config.json"
				.AddCommandLine(args)
				//.AddInMemoryCollection(dict)
				.Build();

			// Console.WriteLine($"Hello {Configuration["Profile:MachineName"]}");
			//  var left = Configuration.GetValue<int>("App:MainWindow:Left", 80);

			SqlQueryExecute();

			Console.WriteLine("\r\nPress any key to finish!");
			Console.ReadKey();
		}

		static bool SqlQueryExecute()
		{
			var availableDatabases = new string[]
			{
				"data",
				"data-light",
				"data-extra-light"
			};

			var sw = new System.Diagnostics.Stopwatch();

			CsvDb.CsvDb db = null;

			bool IsObjectNull(object obj, string msg, bool testFor = true)
			{
				if ((obj == null) == testFor)
				{
					Console.WriteLine(msg);
					return true;
				}
				return false;
			}

			//Action displayHelp = () =>
			void displayHelp()
			{
				// (H)elp
				// (C)lear console
				// (Q)uit
				//
				// (U)se [database].[table]
				// (K)ill/close database
				// (S)tructure									-database info
				// (D)isplay [table]						-table info
				// (D)isplay [table].column			-table column info
				// (S)earch											-query search database
				// (L)ist												-list available databases
				// (L)ist [table]								-list al records of table
				//
				Console.WriteLine("");
				Console.WriteLine("Menu");
				Console.WriteLine(" (H)elp");
				Console.WriteLine(" Clea(r)");
				Console.WriteLine(" (Q)uit");

				Console.WriteLine(" (D)isplay available database(s)");
				Console.WriteLine(" (M)ount database");
				Console.WriteLine(" (K)ill/close database");
				Console.WriteLine(" (S)earch Database");
				Console.WriteLine(" Display (I)nformation of Database");
				Console.WriteLine(" Display (T)ables");
				Console.WriteLine(" Display (C)olumn");

				Console.WriteLine(" Display Index Tree (N)ode Structure");
				Console.WriteLine(" Display (I)ndex Tree Structure");
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
					case ConsoleKey.I:
						//display index structure
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							Console.Write("[table].index: ");
							var tbleColumn = Console.ReadLine();
							var vis = new Visualizer(db);
							vis.DisplayItemsPageStructureInfo(tbleColumn);
						}
						break;
					case ConsoleKey.N:
						//display index structure
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							Console.Write("[table].index: ");
							var tbleColumn = Console.ReadLine();
							var vis = new Visualizer(db);
							vis.DisplayTreeNodePageStructureInfo(tbleColumn);
						}
						break;
					case ConsoleKey.M:
						if (!IsObjectNull(db, $"\r\nplease first unmount current database", testFor: false))
						{
							Console.Write("database name >");
							var dbName = Console.ReadLine();
							if ((db = OpenDatabase(dbName: dbName, logTimes: true)) != null)
							{
								Console.WriteLine($"\r\nUsing database: {db.Name}");
							}
						}
						break;
					case ConsoleKey.K:
						if (!IsObjectNull(db, $" no database to close"))
						{
							Console.WriteLine($" closing database [{db.Name}]");
							db.Dispose();
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
						if (!IsObjectNull(db, " there's no database in use"))
						{
							foreach (var t in db.Tables)
							{
								var text = $" ({t.Columns.Count}) {t.Name}{(t.Multikey ? " Multikey " : "")}{(t.Rows > 0 ? $" {t.Rows} row(s)" : "")}";
								Console.WriteLine(text.ToLower());
							}
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
							if (!IsObjectNull(db, " there's no database in use"))
							{
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
										var text = $" [{table.Name}].{col.Name}: {col.Type}" +
										 $"  {(col.Key ? " Key" : "")}{(col.Indexed ? " Indexed" : "")}{(col.Unique ? " Unique" : "")} {(col.PageCount > 0 ? $" {col.PageCount} page(s)" : "")}";
										Console.WriteLine(text.ToLower());
									}
								}
							}
						}
						break;
					case ConsoleKey.S:
						try
						{
							if (!IsObjectNull(db, " there's no database in use"))
							{
								Console.Write(" query >");
								var query = Console.In.ReadLine();
								Console.WriteLine();
								//Console.WriteLine($" processing: {query}");

								var parser = new CsvDbQueryParser();
								sw.Start();
								var dbQuery = parser.Parse(db, query);
								sw.Stop();
								Console.WriteLine(" query parsed on {0} ms", sw.ElapsedMilliseconds);

								//to calculate times
								sw.Restart();

								var visualizer = new CsvDbVisualizer(dbQuery);
								var rows = visualizer.Execute();

								sw.Stop();
								var rowCount = rows.Count();
								Console.Write($" {rowCount} row(s) ");
								Console.WriteLine("retrieved on {0} ms", sw.ElapsedMilliseconds);

								//header
								sw.Restart();

								var header = String.Join("|", dbQuery.Columns.Header);
								Console.WriteLine($"\r\n{header}");
								Console.WriteLine($"{new string('-', header.Length)}");

								foreach (var record in rows)
								{
									Console.WriteLine($"{String.Join(",", record)}");
								}

								sw.Stop();
								Console.WriteLine("\r\n displayed on {0} ms", sw.ElapsedMilliseconds);
							}
						}
						catch (Exception ex)
						{
							Console.WriteLine($"error: {ex.Message}");
						}
						break;
					case ConsoleKey.Q:
						end = true;
						if (db != null)
						{
							db.Dispose();
						}
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

		static CsvDb.CsvDb OpenDatabase(string dbName = null, bool logTimes = true)
		{
			//var section = Program.Configuration.GetSection("Data");
			var appConfig = Configuration.GetSection("App").Get<Config.AppSettings>();
			var basePath = appConfig.Database.BasePath; // section["BasePath"];

			if (string.IsNullOrWhiteSpace(dbName))
			{
				dbName = "data\\";
			}
			string rootPath = $"{basePath}{dbName}";

			if (!rootPath.EndsWith('\\'))
			{
				rootPath += "\\";
			}

			System.Diagnostics.Stopwatch sw = null;
			if (logTimes)
			{
				sw = new System.Diagnostics.Stopwatch();
				sw.Start();
			}

			CsvDb.CsvDb db = null;
			try
			{
				db = new CsvDb.CsvDb(rootPath);

				if (logTimes)
				{
					sw.Stop();
					Console.WriteLine(" opened on {0} ms", sw.ElapsedMilliseconds);
				}
			}
			catch (Exception ex)
			{
				db = null;
				Console.WriteLine($"error: {ex.Message}");
			}
			return db;
		}

	}
}
