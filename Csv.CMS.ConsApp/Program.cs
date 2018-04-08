using CsvDb;
using CsvDb.Query;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using con = System.Console;

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

			// con.WriteLine($"Hello {Configuration["Profile:MachineName"]}");
			//  var left = Configuration.GetValue<int>("App:MainWindow:Left", 80);

			SqlQueryExecute();

			con.WriteLine("\r\nPress any key to finish!");
			con.ReadKey();
		}

		static bool SqlQueryExecute()
		{
			var availableDatabases = new string[]
			{
				"data-full",
				//"data",
				//"data-light",
				//"data-extra-light",
				"data-bin"
			};

			var sw = new System.Diagnostics.Stopwatch();

			CsvDb.CsvDb db = null;

			bool IsObjectNull(object obj, string msg, bool testFor = true)
			{
				if ((obj == null) == testFor)
				{
					con.WriteLine(msg);
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
					con.WriteLine($"error: {ex.Message}");
					return null;
				}
			}

			//Action displayHelp = () =>
			void displayHelp()
			{
				con.WriteLine("	┌────────────────────────────────┬─────────────────────────────┬─────────────────────────────┐");
				con.WriteLine("	│ (H)elp                         │ Clea(r)                     │ (Q)uit                      │");
				con.WriteLine("	├────────────────────────────────┴─────────────┬───────────────┴─────────────────────────────┤");
				con.WriteLine("	│ (D)isplay available database(s)              │                                             │");
				con.WriteLine("	│ (M)ount database                             │ (K)ill/close database                       │");
				con.WriteLine("	│ (S)earch Database                            │ (E)execute Queries                          │");
				con.WriteLine("	│ Display (I)nformation of Table.Column        │ Display (T)ables Info                       │");
				con.WriteLine("	│ Display Index Tree (N)ode Structure          │ Display (I)ndex Tree Structure              │");
				con.WriteLine("	│ (X)treme class                               │                                             │");
				con.WriteLine("	├──────────────────────────────────────────────┴─────────────────────────────────────────────┤");
				con.WriteLine("	│  SELECT [*] | [t0.col0, t0.col1,..] | [COUNT|AVG|SUM](col)                                 │");
				con.WriteLine("	│      FROM table [t0]                                                                       │");
				con.WriteLine("	│      WHERE                                                                                 │");
				con.WriteLine("	│      [INNER|CROSS|(LEFT|RIGHT|FULL) OUTER] JOIN table0 t0 ON expr:<left> oper <right>      │");
				con.WriteLine("	└────────────────────────────────────────────────────────────────────────────────────────────┘");

				// ORDER BY 

				//	SELECT * FROM [table] t
				//		WHERE t.Name == ""
				//
				// *\t[table]\t[[column],==,[value]>]
				//SELECT route_id, rout_short_name FROM routes r
				//		WHERE r.agency_id == "NJT" AND
				//					r.route_type == 2
				// route_id,route_short_name\t[routes]\t[agency_id],==,"NJT"\tAND\t[route_type],==,2

			}

			System.Reflection.Assembly assembly = null;

			//con.TreatControlCAsInput = true;
			bool end = false;
			//displayHelp();
			while (!end)
			{
				con.Write(">");

				while (con.KeyAvailable == false)
					Thread.Sleep(250); // Loop until input is entered.

				ConsoleKeyInfo key = con.ReadKey();
				con.WriteLine();
				switch (key.Key)
				{
					case ConsoleKey.X:
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							con.Write("\r\n  database table as class: ");
							var tbleName = con.ReadLine();
							DbTable table = db.Table(tbleName);
							//
							if (table == null)
							{
								con.WriteLine($"cannot find table [{tbleName}]");
							}
							else
							{
								if (assembly == null)
								{
									assembly = Utils.CreateDbClasses(db);
									if (assembly == null)
									{
										con.WriteLine("Cannot generate database table classes");
									}
									else
									{
										con.WriteLine("database table classes generated succesfully!");
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

									con.WriteLine("ok");
								}
							}

						}
						break;
					case ConsoleKey.E:
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							con.WriteLine("Execute database queries:\r\n  -empty query ends");
							string query = null;
							bool finish = false;
							do
							{
								con.Write("\r\n  query: ");
								query = con.ReadLine();
								if (!(finish = String.IsNullOrWhiteSpace(query)))
								{
									try
									{
										var parser = new DbQueryParser();
										sw.Restart();
										var dbQuery = parser.Parse(db, query);
										sw.Stop();
										con.WriteLine("    query parsed on {0} ms", sw.ElapsedMilliseconds);
										con.WriteLine($"     {dbQuery}");
									}
									catch (Exception ex)
									{
										con.WriteLine($"    error: {ex.Message}");
									}
								}
							} while (!finish);
						}
						break;
					case ConsoleKey.I:
						//display index structure
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							con.Write("[table].index: ");
							var tbleColumn = con.ReadLine();
							var vis = new Visualizer(db);
							vis.DisplayItemsPageStructureInfo(tbleColumn);
						}
						break;
					case ConsoleKey.N:
						//display index structure
						if (!IsObjectNull(db, $"\r\nno database in use to show info"))
						{
							con.Write("[table].index: ");
							var tbleColumn = con.ReadLine();
							var vis = new Visualizer(db);
							vis.DisplayTreeNodePageStructureInfo(tbleColumn);
						}
						break;
					case ConsoleKey.M:
						if (!IsObjectNull(db, $"\r\nplease first unmount current database", testFor: false))
						{
							con.Write("database name >");
							var dbName = con.ReadLine();
							if ((db = OpenDatabase(dbName: dbName, logTimes: true)) != null)
							{
								con.WriteLine($"\r\nUsing database: {db.Name}{db.IsBinary.IfTrue(" [Binary]")}{db.IsCsv.IfTrue(" [Csv]")}");
							}
						}
						break;
					case ConsoleKey.K:
						if (!IsObjectNull(db, $" no database to close"))
						{
							con.WriteLine($" closing database [{db.Name}]");
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
						con.WriteLine($" database(s){txt}");
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
								var text = $"\r\n [{table.Name}]{(table.Multikey.IfTrue(" -m "))}{(table.Rows > 0).IfTrue($" {table.Rows} row(s)")}";
								con.WriteLine(text.ToLower());
								//show columns
								foreach (var col in table.Columns)
								{
									text = $"   {col.Name}: {col.Type}" +
									 $"  {(col.Key.IfTrue("-k"))}{(col.Indexed.IfTrue("-i"))}{(col.Unique.IfTrue("-u"))} {(col.PageCount > 0).IfTrue($" {col.PageCount} page(s)")}";
									con.WriteLine(text.ToLower());
								}
							}
						}
						break;
					case ConsoleKey.S:
						try
						{
							if (!IsObjectNull(db, " there's no database in use"))
							{
								con.Write(" query >");
								var query = con.In.ReadLine();
								con.WriteLine();
								//con.WriteLine($" processing: {query}");

								var parser = new DbQueryParser();
								sw.Start();
								var dbQuery = parser.Parse(db, query);
								sw.Stop();
								con.WriteLine(" query parsed on {0} ms", sw.ElapsedMilliseconds);
								con.WriteLine($"  {dbQuery}");

								var visualizer = DbVisualizer.Create(dbQuery,
									DbVisualize.Paged | DbVisualize.UnderlineHeader | DbVisualize.Framed | DbVisualize.LineNumbers);
								visualizer.Display();
								visualizer.Dispose();
							}
						}
						catch (Exception ex)
						{
							con.WriteLine($"error: {ex.Message}");
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
						con.Clear();
						break;
					default:
						con.WriteLine(" -invalid option, press [h] for help");
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
					con.WriteLine(" opened on {0} ms", sw.ElapsedMilliseconds);
				}
			}
			catch (Exception ex)
			{
				db = null;
				con.WriteLine($"error: {ex.Message}");
			}
			return db;
		}

	}
}
