using CsvDb;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
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

#if DEBUG
			Console.WriteLine("Mode=Debug");
#else
    Console.WriteLine("Mode=Release"); 
#endif

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
				con.WriteLine("	│ (P)age                                       │ Display (T)ables Info                       │");
				con.WriteLine("	│ Display Index Tree (N)ode Structure          │ Display (I)ndex Tree Structure              │");
				con.WriteLine("	│ (C)omparer                                   │ (V)isualize record(s)                       │");
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

				// SELECT * | column0,column1,...
				//					|	a.agency_id, b.serice_id,...
				//
				//	FROM table [descriptor]
				//		
				//	WHERE [descriptor].column = constant AND|OR ...
				//	SKIP number
				//	LIMIT number
				//
			}

			System.Reflection.Assembly assembly = null;
			var nl = Environment.NewLine;
			bool end = false;
			//this's the matched rule
			CommandArgRulesAction action = null;

			//con.TreatControlCAsInput = true;

			var actions = new CommandArgRules(
				new CommandArgRulesAction[]
				{
					new CommandArgRulesAction(
					() =>
						{
							end = true;
							if (db != null)
							{
								db.Dispose();
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => new string[] { "quit", "q" }.Contains( arg.Key) && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() => displayHelp(),
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => new string[] { "help", "h"}.Contains( arg.Key) && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() => con.Clear(),
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => new string[] { "c", "clear" }.Contains( arg.Key) && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							if (!IsObjectNull(db, $" no database to close"))
							{
								con.WriteLine($" closing database [{db.Name}]");
								db.Dispose();
								db = null;
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => new string[] { "k", "kill" }.Contains( arg.Key) && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//m "data-bin"
							if (!IsObjectNull(db, $"\r\nplease first unmount current database", testFor: false))
							{
								if ((db = OpenDatabase(
									dbName: action.Arguments.FirstOrDefault().Arg.GetKey(),
									logTimes: true)) != null)
								{
									con.WriteLine($"\r\nUsing database: {db.Name}{db.IsBinary.IfTrue(" [Binary]")}{db.IsCsv.IfTrue(" [Csv]")}");
								}
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => new string[] { "m", "mount", "use" }.Contains( arg.Key) && !arg.IsKeyPair),
							new CommandArgRule(1, (arg) => arg.Type == CommandArgItemType.Identifier ||
								arg.Type == CommandArgItemType.String)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							if (!IsObjectNull(db, " there's no database in use"))
							{
								con.Write(" query >");
								var query = con.In.ReadLine();
								con.WriteLine();

								sw.Restart();
								var dbQuery = DbQuery.Parse(query, new CsvDbDefaultValidator(db));
								sw.Stop();
								con.WriteLine(" query parsed on {0} ms", sw.ElapsedMilliseconds);
								con.WriteLine($"  {dbQuery}");

								var visualizer = new DbVisualizer(
									db,
									dbQuery,
									DbVisualize.Paged |
										DbVisualize.UnderlineHeader |
										DbVisualize.Framed |
										DbVisualize.LineNumbers);
								visualizer.Display();
								visualizer.Dispose();
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "search" && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							if (!IsObjectNull(db, " there's no database in use"))
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
											sw.Restart();
											var dbQuery = DbQuery.Parse(query, new CsvDbDefaultValidator(db));
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
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "execute" && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							if (!IsObjectNull(db, " there's no database in use"))
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
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "x" && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//compare
							//display "routes.route_id" /oper:>= 250
							if (!IsObjectNull(db, " there's no database in use"))
							{
								var dbTblCol = action.Arguments.FirstOrDefault(r => r.Id == 1).Arg.GetKey();

								var operArg = action.Arguments.FirstOrDefault(r => r.Id == 2).Arg as CommandArgKeypair;

								var constArg = action.Arguments.FirstOrDefault(r => r.Id == 2).Arg;

								DbColumn column = null;
								if ((column = db.Index(dbTblCol)) != null &&
									operArg.Value.TryParseToken(out TokenType token))
								{
									var reader = DbTableDataReader.Create(db, column.Table);

									object value = column.TypeEnum.ToObject(constArg.Key);

									var collection = (IEnumerable<int>)Utils.CallGeneric(
										reader, nameof(DbTableDataReader.Compare), column.TypeEnum,
										new object[] { column, token, value },
										 System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance
									);
									var lista = collection.ToList();
									con.WriteLine($" ({lista.Count}) match(es)");
								}
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg) => arg.Type == CommandArgItemType.String && !arg.IsKeyPair),
							new CommandArgRule(2, (arg) => arg.Key == "/oper" && arg.IsKeyPair),
							new CommandArgRule(3, (arg) => arg.Type == CommandArgItemType.Integer || arg.Type == CommandArgItemType.String)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							con.WriteLine($" databases:{nl}  {String.Join($"{nl}  ", availableDatabases)}");
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							if (!IsObjectNull(db, " there's no database in use"))
							{
								ShowAllTableInfo(db);
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg) => arg.Key =="/tables" && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//display index structure
							//display /i "trips.trip_id"
							if (!IsObjectNull(db, $"\r\nno database in use to show info"))
							{
								var vis = new Visualizer(db);
								vis.DisplayItemsPageStructureInfo(
									action.Arguments.FirstOrDefault(r => r.Id == 2).Arg.GetKey()
								);
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg)=> arg.Key== "/i" && !arg.IsKeyPair),
							new CommandArgRule(2, (arg) => arg.Type == CommandArgItemType.String && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//display index structure
							//display /n "trips.trip_id"
							if (!IsObjectNull(db, $"\r\nno database in use to show info"))
							{
								var vis = new Visualizer(db);
								vis.DisplayTreeNodePageStructureInfo(
									action.Arguments.FirstOrDefault(r => r.Id == 2).Arg.GetKey()
								);
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg)=> arg.Key== "/n" && !arg.IsKeyPair),
							new CommandArgRule(2, (arg) => arg.Type == CommandArgItemType.String && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//display index page
							//display /p "trips.trip_id" /offset:8
							if (!IsObjectNull(db, $"\r\nno database in use to show info"))
							{
								DbColumn column = null;
								if (!int.TryParse(action.Arguments.FirstOrDefault(r => r.Id == 3).Arg.GetValue(),
														out int offset) ||
									(column = db.Index(action.Arguments.FirstOrDefault( r => r.Id == 2).Arg.GetKey())) == null)
								{
									con.WriteLine(" \r\n error: invalid table column or id/offset of page");
								}
								else
								{
									Utils.CallGeneric(new Visualizer(db),
										nameof(Visualizer.ShowItemPage),
										column.TypeEnum,
										new object[] { column, offset });
								}
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg)=> arg.Key== "/p" && !arg.IsKeyPair),
							new CommandArgRule(2, (arg) => arg.Type == CommandArgItemType.String && !arg.IsKeyPair),
							new CommandArgRule(3, (arg) => arg.Key == "/offset" && arg.IsKeyPair &&
								((CommandArgKeypair)arg).ValueType == CommandArgItemType.Integer)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//display index page
							//display /r "trips.trip_id" 12
							if (!IsObjectNull(db, $"\r\nno database in use to show info"))
							{
								DbColumn column = null;
								if (!int.TryParse(action.Arguments.FirstOrDefault(r => r.Id == 3).Arg.GetKey(), 
												out int count) ||
									(column = db.Index(
											action.Arguments.FirstOrDefault( r => r.Id == 2).Arg.GetKey()
										)) == null)
								{
									con.WriteLine(" \r\n  error: invalid data");
								}
								else
								{
									Utils.CallGeneric(new Visualizer(db),
										nameof(Visualizer.ShowTableColumnRows),
										column.TypeEnum,
										new object[] { column, count });
								}
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "display" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg)=> arg.Key== "/r" && !arg.IsKeyPair),
							new CommandArgRule(2, (arg) => arg.Type == CommandArgItemType.String && !arg.IsKeyPair),
							new CommandArgRule(3, (arg) => arg.Type == CommandArgItemType.Integer && !arg.IsKeyPair)
						}
					),
					new CommandArgRulesAction(
						() =>
						{
							//display index page
							//test /t:1
							if (!IsObjectNull(db, $"\r\nno database in use to show info"))
							{
								var agencyType = db.Classes["agency"];
								var agencyObject = agencyType != null ? Activator.CreateInstance(agencyType.Type) : null;

								//var agencyId_Prop = agencyType.Type.GetProperty("agency_id");
								//agencyId_Prop.SetValue(agencyObject, "NJT");

								var agencyApply = agencyType.Type.GetMethod("Apply");
								agencyApply.Invoke(agencyObject, new object[] {
									agencyType.Props,
									new object[] {
										"NJB", "NJ TRANSIT BUS", "http://www.njtransit.com/", "America/New_York", "en", null
									}});
								var ts = agencyType.Type.GetMethod("ToString", new Type[] { typeof(DbProperty[]) });
								if (ts != null)
								{
									var s = ts.Invoke(agencyObject, new object[] { agencyType.Props });
									con.WriteLine(s);
								}
								con.WriteLine($" generated agency class: {(agencyObject != null).ToYesNo()}");
							}
						},
						new CommandArgRule[]
						{
							new CommandArgRule(0, (arg) => arg.Key == "test" && !arg.IsKeyPair),
							new CommandArgRule(1, (arg)=> arg.Key== "/t:1" && arg.IsKeyPair)
						}
					)
				}
			);

			while (!end)
			{
				con.Write(">");

				var args = new CommandLineParser(con.ReadLine()).Arguments();

				//here we should have only one rule match
				var matches = actions.Actions.Where(a => a.Match(args)).ToList();
				if (matches.Count != 1)
				{
					con.WriteLine(" no command or too many matches");
				}
				else
				{
					(action = matches[0]).Action?.Invoke();
				}
				//clear action rules
				actions.Clear();
			}
			return true;
		}

		static void ShowAllTableInfo(CsvDb.CsvDb db)
		{
			var tablesInfo =
				from table in db.Tables
				select new
				{
					name = table.Name,
					flags = table.Multikey.IfTrue("-m"),
					rows = $"{table.Rows.ToString("##,#")} row(s)",
					mask = table.RowMask,
					masklen = table.RowMaskLength,
					columnRows =
						(from col in table.Columns
						 select new List<KeyValuePair<string, string>>()
							{
								new KeyValuePair<string,string>("column", col.Name),
								new KeyValuePair<string,string>("type",  col.Type),
								new KeyValuePair<string,string>("flags", $"{(col.Key.IfTrue("-k"))}{(col.Indexed.IfTrue("-i"))}{(col.Unique.IfTrue("-u"))}"),
								new KeyValuePair<string,string>("pages", col.PageCount.ToString())
							}
						).ToList()
				};

			//standarize all columns
			var columnWidths = new int[4];
			foreach (var t in tablesInfo)
			{
				//calculate column widths
				foreach (var col in t.columnRows)
				{
					for (var i = 0; i < col.Count; i++)
					{
						columnWidths[i] = Math.Max(columnWidths[i], Math.Max(col[i].Key.Length, col[i].Value.Length));
					}
				}
			}
			//space column name
			columnWidths[0] += 12;

			//calculate with spaces and frames
			var maxTableWidth = columnWidths.Sum((w) => w + 1 + 1 + 1) + 1;

			//visualize
			foreach (var t in tablesInfo)
			{
				//first line
				con.WriteLine($"┌{new String('─', maxTableWidth - 2)}┐");
				//table name
				var text = $"{t.flags} {t.rows}";
				con.WriteLine($"│{t.name}{(new string(' ', maxTableWidth - t.name.Length - text.Length - 2))}{text}│");
				//columns
				text = $"├{String.Join('┬', columnWidths.Select(w => new String('─', w + 2)))}┤";
				con.WriteLine(text);
				//header
				con.WriteLine($"│{String.Join('│', t.columnRows[0].Select((k, ndx) => k.Key.PadRight(columnWidths[ndx] + 2)))}│");
				//separator
				con.WriteLine(text.Replace('┬', '┼'));
				//rows with columns
				foreach (var col in t.columnRows)
				{
					con.WriteLine($"│{String.Join('│', col.Select((k, ndx) => k.Value.PadRight(columnWidths[ndx] + 2)))}│");
				}
				//end line
				con.WriteLine(text.Replace('┬', '┴').Replace('├', '└').Replace('┤', '┘'));
				con.WriteLine($"  RowMask: {Convert.ToString((long)t.mask, 2)}   RowMask length: {t.masklen}");
				con.WriteLine();
			}
			//reference
			con.WriteLine("ref:   -m MultiKey   -k Key   -i Indexed   -u Unique ");
		}

		static CsvDb.CsvDb OpenDatabase(string dbName = null, bool logTimes = true)
		{
			//var section = Program.Configuration.GetSection("Data");
			var appConfig = Configuration.GetSection("App").Get<Config.AppSettings>();
			var basePath = appConfig.Database.BasePath; // section["BasePath"];

			if (string.IsNullOrWhiteSpace(dbName))
			{
				dbName = "data-bin\\";
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