using CsvDb;
using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace ConsoleApp
{
	class Program
	{
		public static IConfiguration Configuration { get; set; }

		static void Main(string[] args)
		{
			Configuration = new ConfigurationBuilder()
				.SetBasePath(System.IO.Directory.GetCurrentDirectory())
				.AddJsonFile("config.json")
				.AddCommandLine(args)
				.Build();

			var handledWait = false;

			Generators();

			//SqlQueryExecuteTests();

			//handledWait = SqlQueryFinalParseTests();

			//TreeTests();

			if (!handledWait)
			{
				Console.WriteLine("\r\nPress enter to finish!");
				Console.ReadLine();
			}
		}

		static CsvDb.CsvDb OpenDatabase(string dbName = null, bool logTimes = true)
		{
			//var section = Program.Configuration.GetSection("Data");
			var appConfig = Configuration.GetSection("App").Get<Config.ConfigSettings>();
			// section["BasePath"];
			var basePath = appConfig.Database.BasePath;

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
					Console.WriteLine("  opened on {0} ms", sw.ElapsedMilliseconds);
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

			//data
			//data-full
			//data-light
			//data-extra-light
			var db = OpenDatabase(dbName: "data-full");

			var appConfig = Configuration.GetSection("App").Get<Config.ConfigSettings>();
			// section["BasePath"];
			var basePath = appConfig.Database.BasePath;

			var gen = new CsvDbGenerator(
				db,
				//$@"{basePath}bus_data.zip",
				$@"{basePath}bus_data-full.zip",
				//$@"{basePath}bus_data-light.zip",
				//$@"{basePath}bus_data-extra-light.zip",

				//comment this when uncommented: gen.GenerateTxtData(); for a clean output
				removeAll: false
			);
			gen.GenerateTxtData();

			gen.CompilePagers();

			gen.CompileIndexes();
		}

		static void SqlQueryExecuteTests()
		{
			var sw = new System.Diagnostics.Stopwatch();
			sw.Start();

			var db = OpenDatabase();

			sw.Stop();
			Console.WriteLine($"\r\nDatabase: {db.Name}");
			Console.WriteLine(">created on {0} ms", sw.ElapsedMilliseconds);

			var query = //"SELECT * FROM routes WHERE route_id >= (Int32)5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> \"NJB\" SKIP 2 LIMIT 5"
									//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id = 180"
									//		180,NJB,74,,3,,
				"SELECT * FROM trips WHERE route_id = 204 AND service_id = 5 AND trip_id = 52325"
				//		204,5,52325,"810 WOODBRIDGE CENTER-Exact Fare",1,810MX003,6369
				;

			Console.WriteLine($"\r\nIn query: {query}");

			sw.Restart();
			var parser = new CsvDbQueryParser();
			var dbQuery = parser.Parse(db, query); // old one: CsvDb.CsvDbQuery.Parse(db, query);

			sw.Stop();
			Console.WriteLine(">parsed on {0} ms", sw.ElapsedMilliseconds);

			var outQuery = dbQuery.ToString();

			Console.WriteLine($"\r\nOut query: {outQuery}\r\n");

			//to calculate times
			sw.Restart();

			var visualizer = new CsvDbVisualizer(dbQuery);
			var rows = visualizer.Execute().ToList();

			sw.Stop();
			Console.WriteLine(">{rows.Count} row(s) retrieved on {0} ms", sw.ElapsedMilliseconds);

			//header
			sw.Restart();

			var header = String.Join("|", dbQuery.Columns.Header);
			Console.WriteLine($"\r\n{header}");
			Console.WriteLine($"{new string('-', header.Length)}");

			foreach (var record in visualizer.Execute())
			{
				Console.WriteLine($"{String.Join(",", record)}");
			}

			sw.Stop();
			Console.WriteLine("\r\n>row(s) displayed on {0} ms", sw.ElapsedMilliseconds);
		}

		static bool SqlQueryFinalParseTests()
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
			return false;
		}

		static void TreeTests()
		{
			var root = new MyTree<char>('A',
				new MyTree<char>('B',
					new MyTree<char>('D',
						new MyTree<char>('F')),
					new MyTree<char>('E')),
				new MyTree<char>('C',
					new MyTree<char>('G'),
					new MyTree<char>('H')));

			Console.WriteLine("Pre Order");
			root.PreOrder((n) =>
			{
				Console.WriteLine(n.Value);
			});

			Console.WriteLine("In Order");
			root.InOrder((n) =>
			{
				Console.WriteLine(n.Value);
			});

			Console.WriteLine("Post Order");
			root.PostOrder((n) =>
			{
				Console.WriteLine(n.Value);
			});
		}

		class MyTree<T>
		{
			public T Value { get; set; }

			public MyTree<T> Left { get; set; }

			public MyTree<T> Right { get; set; }

			public MyTree(T value, MyTree<T> left = null, MyTree<T> right = null)
			{
				Value = value;
				Left = left;
				Right = right;
			}

			public override string ToString() => $"{Value}";

			public void PreOrder(Action<MyTree<T>> action)
			{
				//https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
				var stack = new Stack<MyTree<T>>();
				//push root
				stack.Push(this);

				while (stack.Count > 0)
				{
					//
					var node = stack.Pop();
					action(node);
					//
					if (node.Right != null)
					{
						stack.Push(node.Right);
					}
					if (node.Left != null)
					{
						stack.Push(node.Left);
					}

				}
			}

			public void InOrder(Action<MyTree<T>> action)
			{
				//https://articles.leetcode.com/binary-search-tree-in-order-traversal/
				var stack = new Stack<MyTree<T>>();

				//set current to root
				MyTree<T> current = this;

				while (stack.Count > 0 || current != null)
				{
					if (current != null)
					{
						stack.Push(current);
						//try to go Left
						current = current.Left;
					}
					else
					{
						current = stack.Pop();
						//call action
						action(current);
						//try to go Right
						current = current.Right;
					}
				}
			}

			public void PostOrder(Action<MyTree<T>> action)
			{
				//postfix
				//https://articles.leetcode.com/binary-tree-post-order-traversal/
				var stack = new Stack<MyTree<T>>();
				//push root
				stack.Push(this);

				MyTree<T> prev = null;

				while (stack.Count > 0)
				{
					var curr = stack.Peek();

					if (prev == null || prev.Left == curr || prev.Right == curr)
					{
						if (curr.Left != null)
							stack.Push(curr.Left);
						else if (curr.Right != null)
							stack.Push(curr.Right);
					}
					else if (curr.Left == prev)
					{
						if (curr.Right != null)
							stack.Push(curr.Right);
					}
					else
					{
						action(curr);
						stack.Pop();
					}
					prev = curr;

				}
			}

		}
	}

}
