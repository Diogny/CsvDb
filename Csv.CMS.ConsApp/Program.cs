using CsvDb;
using CsvDb.Query;
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
				"data-full",
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

			object CreateClass(Type type, object[] parameters)
			{
				if (type == null)
				{
					return null;
				}
				try
				{
					object obj = Activator.CreateInstance(type, parameters ?? new object[] { });
					return obj;
				}
				catch (Exception ex)
				{
					Console.WriteLine($"error: {ex.Message}");
					return null;
				}
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
				Console.WriteLine(" Display (T)ables Info");
				Console.WriteLine(" Display Index Tree (N)ode Structure");
				Console.WriteLine(" Display (I)ndex Tree Structure");
				Console.WriteLine(" (E)execute Queries");
				Console.WriteLine(" (X)treme class");
			}

			System.Reflection.Assembly assembly = null;

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
					case ConsoleKey.X:
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							Console.Write("\r\n  database table as class: ");
							var tbleName = Console.ReadLine();
							DbTable table = db.Table(tbleName);
							//
							if (table == null)
							{
								Console.WriteLine($"cannot find table [{tbleName}]");
							}
							else
							{
								if (assembly == null)
								{
									assembly = Utils.CreateDbClasses(db);
									if (assembly == null)
									{
										Console.WriteLine("Cannot generate database table classes");
									}
									else
									{
										Console.WriteLine("database table classes generated succesfully!");
									}
								}
								if (assembly != null)
								{
									//I can compile code once and load assembly at start
									//   just recompile when database table changes

									//var an = System.Reflection.AssemblyName.GetAssemblyName(filePath);
									//System.Reflection.Assembly.Load(an);
									//AppDomain.CurrentDomain.Load(assembly.GetName());

									//get it OK!
									//Type type = assembly.GetType($"CsvDb.Dynamic.{tbleName}");
									//object obj = Activator.CreateInstance(type);

									//this was a test, constructor must be parameterless so CsvHelper can create it
									Type dynTbleClass = assembly.GetType($"CsvDb.Dynamic.{tbleName}");
									object obj = CreateClass(
										dynTbleClass,
										new object[] {
											//table
										}
									);
									var mthd = dynTbleClass.GetMethod("Link");
									mthd.Invoke(obj, new object[]
									{
										table
									});
									//now I can use CsvHelper to parse CSV rows using this classes if needed

									//don't get it
									var classType = Type.GetType($"CsvDb.Dynamic.{tbleName}");

									Console.WriteLine("ok");
								}
							}

						}
						break;
					case ConsoleKey.E:
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							Console.WriteLine("Execute database queries:\r\n  -empty query ends");
							string query = null;
							bool finish = false;
							do
							{
								Console.Write("\r\n  query: ");
								query = Console.ReadLine();
								if (!(finish = String.IsNullOrWhiteSpace(query)))
								{
									try
									{
										var parser = new DbQueryParser();
										sw.Restart();
										var dbQuery = parser.Parse(db, query);
										sw.Stop();
										Console.WriteLine("    query parsed on {0} ms", sw.ElapsedMilliseconds);
										Console.WriteLine($"     {dbQuery}");
									}
									catch (Exception ex)
									{
										Console.WriteLine($"    error: {ex.Message}");
									}
								}
							} while (!finish);
						}
						break;
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
							foreach (var table in db.Tables)
							{
								var text = $" [{table.Name}]{(table.Multikey.IfYes(" [Multikey] "))}{(table.Rows > 0 ? $" {table.Rows} row(s)" : "")}";
								Console.WriteLine(text.ToLower());
								//show columns
								foreach (var col in table.Columns)
								{
									text = $"   {col.Name}: {col.Type}" +
									 $"  {(col.Key.IfYes(" [Key]"))}{(col.Indexed.IfYes(" [Indexed]"))}{(col.Unique.IfYes(" [Unique]"))} {(col.PageCount > 0 ? $" {col.PageCount} page(s)" : "")}";
									Console.WriteLine(text.ToLower());
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

								var parser = new DbQueryParser();
								sw.Start();
								var dbQuery = parser.Parse(db, query);
								sw.Stop();
								Console.WriteLine(" query parsed on {0} ms", sw.ElapsedMilliseconds);
								Console.WriteLine($"  {dbQuery}");

								//to calculate times
								sw.Restart();

								var visualizer = new DbVisualizer(dbQuery);
								var rows = visualizer.Execute();
								visualizer.Dispose();

								sw.Stop();
								var rowCount = rows.Count();
								Console.Write($" {rowCount} row(s) ");
								Console.WriteLine("retrieved on {0} ms", sw.ElapsedMilliseconds);

								//header
								sw.Restart();

								var header = String.Join("|", dbQuery.Columns.Header);
								Console.WriteLine($"\r\n{header}");
								Console.WriteLine($"{new string('-', header.Length)}");

								int visualizedRows = 0;
								int pagerRows = 0;
								var unstop = false;
								foreach (var record in rows)
								{
									visualizedRows++;
									Console.WriteLine($"{String.Join(",", record)}");

									if (!unstop && pagerRows++ >= 32)
									{
										pagerRows = 0;
										Console.Write("press any key...");
										var keyCode = Console.ReadKey();
										Console.WriteLine();
										if (keyCode.Key == ConsoleKey.Escape)
										{
											unstop = true;
										}
									}
								}

								sw.Stop();
								Console.WriteLine($"\r\n {visualizedRows} row(s) displayed on {0} ms", sw.ElapsedMilliseconds);
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
