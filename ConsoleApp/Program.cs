using CsvDb;
using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using io = System.IO;

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

			var appConfig = Configuration.GetSection("App").Get<Config.ConfigSettings>();

			var handledWait = false;

			//string dbName = "data-bin";
			////Create Db with binary table rows and indexes
			////data
			////data-light
			////data-extra-light
			////data-bin,			DbSchemaConfigEnum.Binary,  "bus_data-full.zip"
			////data-full,		DbSchemaConfigEnum.Csv,		"bus_data-full.zip"
			//GenerateInitialDbData(appConfig, dbName, DbSchemaConfigType.Binary, "bus_data.zip");

			////compile indices
			//CompileDb(appConfig, dbName);

			//SqlQueryExecuteTests();

			//handledWait = SqlQueryFinalParseTests();

			//JsonSchemas();

			//TreeTests();

			//TestDynamic();

			//CsvToBin();

			ShowAllTablesAndColumns(appConfig);

			if (!handledWait)
			{
				Console.WriteLine("\r\nPress enter to finish!");
				Console.ReadLine();
			}
		}

		static void JsonSchemas()
		{
			var schema = DbGenerator.Schema();
			Console.WriteLine(schema);
		}

		static CsvDb.CsvDb OpenDatabase(Config.ConfigSettings appConfig, string dbName = null, bool logTimes = true)
		{
			var basePath = appConfig.Database.BasePath;

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

		static void GenerateInitialDbData(
			Config.ConfigSettings appConfig,
			string dbname,
			DbSchemaConfigType dbConfig,
			string zipfile
		)
		{
			var basePath = appConfig.Database.BasePath;

			var jsonfilepath = $"{basePath}\\{dbname}\\bin\\__tables.init.json";

			var db = CsvDb.CsvDb.CreateFromJson(jsonfilepath, dbConfig);

			var gen = new DbGenerator(db, $@"{basePath}{zipfile}", removeAll: false);

			gen.Generate();
		}

		static void CompileDb(Config.ConfigSettings appConfig, string dbname)
		{
			var db = OpenDatabase(appConfig, dbName: dbname);

			var basePath = appConfig.Database.BasePath;

			var gen = new DbGenerator(db, removeAll: false);

			gen.CompilePagers();

			gen.Compile();
		}

		static void ShowAllTablesAndColumns(Config.ConfigSettings appConfig)
		{
			var db = OpenDatabase(appConfig);

			Console.WriteLine("switch (tableName)\r\n{");
			foreach (var table in db.Tables)
			{
				Console.WriteLine($"\tcase \"{table.Name}\":");
				Console.WriteLine($"\t\treturn new KeyValuePair<string, string>[] {{ {String.Join(", ", table.Columns.Select(c => $"new KeyValuePair<string, string>(\"{c.Name}\", \"{c.Type}\")"))} }}");
				Console.WriteLine($"\t\t\t.Contains(columnName);");
			}
			Console.WriteLine("\tdefault:");
			Console.WriteLine("\t\treturn false;");
			Console.WriteLine("}");
		}

		static void SqlQueryExecuteTests(Config.ConfigSettings appConfig)
		{
			//var sw = new System.Diagnostics.Stopwatch();
			//sw.Start();

			//var db = OpenDatabase(appConfig);

			//sw.Stop();
			//Console.WriteLine($"\r\nDatabase: {db.Name}");
			//Console.WriteLine(">created on {0} ms", sw.ElapsedMilliseconds);

			//var query = //"SELECT * FROM routes WHERE route_id >= (Int32)5 AND agency_id <> 'NJB' LIMIT 5"
			//						//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> 'NJB' LIMIT 5"
			//						//"SELECT route_id, agency_id,route_type FROM routes WHERE route_id = 180"
			//						//		180,NJB,74,,3,,
			//	"SELECT * FROM trips WHERE route_id = 204 AND service_id = 5 AND trip_id = 52325"
			//	//		204,5,52325,"810 WOODBRIDGE CENTER-Exact Fare",1,810MX003,6369
			//	;

			//Console.WriteLine($"\r\nIn query: {query}");

			//sw.Restart();
			//var dbQuery = DbQuery.Parse(query, new CsvDbDefaultValidator(db)); // old one: CsvDb.CsvDbQuery.Parse(db, query);

			//sw.Stop();
			//Console.WriteLine(">parsed on {0} ms", sw.ElapsedMilliseconds);

			//var outQuery = dbQuery.ToString();

			//Console.WriteLine($"\r\nOut query: {outQuery}\r\n");

			////to calculate times
			//sw.Restart();

			//var reader = DbRecordReader.Create(db, dbQuery);
			//var rows = reader.Rows().ToList();
			//reader.Dispose();

			//sw.Stop();
			//Console.WriteLine(">{rows.Count} row(s) retrieved on {0} ms", sw.ElapsedMilliseconds);

			////header
			//sw.Restart();

			//var header = String.Join("|", dbQuery.Select.Header);
			//Console.WriteLine($"\r\n{header}");
			//Console.WriteLine($"{new string('-', header.Length)}");

			//foreach (var record in rows)
			//{
			//	Console.WriteLine($"{String.Join(",", record)}");
			//}

			//sw.Stop();
			//Console.WriteLine("\r\n>row(s) displayed on {0} ms", sw.ElapsedMilliseconds);
		}

		static bool SqlQueryFinalParseTests(Config.ConfigSettings appConfig)
		{
			var db = OpenDatabase(appConfig);

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
				"SELECT * FROM routes LIMIT ",
				"SELECT * FROM routes WHERE route_id ",
				"SELECT * FROM routes WHERE route_id > ",
				"SELECT * FROM routes WHERE > 5 LIMIT 2",
				"SELECT * FROM routes WHERE route_id > LIMIT 2",
				//this one is because cookoo_id is not a table
				"SELECT * FROM routes WHERE cookoo_id > 4 LIMIT 2",

				//
				"SELECT * FROM routes",
				"SELECT * FROM routes LIMIT 5",
				"SELECT * FROM routes WHERE route_id >= 5",
				"SELECT * FROM routes WHERE route_id >= 5 AND agency_id <> 'NJB'",
				"SELECT * FROM routes WHERE route_id >= 5 AND agency_id <> 'NJB' LIMIT 5",
				//
				"SELECT route_id, agency_id,route_type FROM routes",
				"SELECT route_id, agency_id,route_type FROM routes LIMIT 2",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> 'NJB'",
				"SELECT route_id, agency_id,route_type FROM routes WHERE route_id >= 5 AND agency_id <> 'NJB' LIMIT 2"
			};

			foreach (var query in queryCollection)
			{
				try
				{
					Console.WriteLine($"\r\n{new string('-', Console.WindowHeight - 1)}");

					Console.WriteLine($"\r\n>{query}");

					var queryDb = DbQuery.Parse(query, new CsvDbDefaultValidator(db));

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

		static void CsvToBin(Config.ConfigSettings appConfig)
		{
			var db = OpenDatabase(appConfig, dbName: "data-full");
			string tableName = null;
			Console.WriteLine("enter table name, empty to exit, prefix with dash - to show percentage ");
			do
			{
				Console.Write("\r\ntable: ");
				tableName = Console.ReadLine();

				var sw = new System.Diagnostics.Stopwatch();
				sw.Start();
				var response = CsvToBinTable(db, tableName);
				sw.Stop();
				if (response)
				{
					Console.WriteLine("  ellapsed {0} ms\r\n\t {1}", sw.ElapsedMilliseconds, sw.Elapsed);
				}
			} while (!String.IsNullOrWhiteSpace(tableName));
		}

		static bool CsvToBinTable(CsvDb.CsvDb db, string tableName)
		{
			////FIND THIS
			//// "stops" table row count csv doesn't match with bin file

			//var showPer = tableName.StartsWith('-');

			//if (showPer)
			//{
			//	tableName = tableName.Substring(1);
			//}

			//var table = db.Table(tableName);
			//if (table == null)
			//{
			//	Console.WriteLine($"cannot find table: {tableName} in database");
			//	return false;
			//}
			////get file size of csv file
			//var csvPath = io.Path.Combine(db.BinaryPath, $"{table.Name}.csv");
			//var csvFileInfo = new io.FileInfo(csvPath);

			//var binPath = io.Path.Combine(db.BinaryPath, $"{table.Name}.bin");

			//if (!showPer)
			//{
			//	//calculate bytes needed to store null flags for every column in every record
			//	//  this way reduce space for many null values on columns
			//	//   and support nulls for any field type
			//	int bytes = Math.DivRem(table.Columns.Count, 8, out int remainder);
			//	int bits = bytes * 8 + remainder;
			//	UInt64 mask = (UInt64)Math.Pow(2, bits - 1);
			//	if (remainder != 0)
			//	{
			//		bytes++;
			//	}

			//	var stream = new io.MemoryStream();
			//	var bufferWriter = new io.BinaryWriter(stream);

			//	using (var writer = new io.BinaryWriter(io.File.Create(binPath)))
			//	//using (var reader = new io.StreamReader(csvPath))
			//	{
			//		//placeholder for row count
			//		Int32 value = 0;
			//		writer.Write(value);

			//		//write mask 32-bits unsigned int 64 bits UInt64, so max table columns is 64
			//		writer.Write(mask);

			//		//var buffer = new io.MemoryStream(5 * 1024);

			//		var queryText = $"SELECT * FROM {table.Name}";
			//		var reader = DbRecordReader.Create(db, DbQuery.Parse(queryText, new CsvDbDefaultValidator(db)));
			//		int rowCount = 0;

			//		foreach (var record in reader.Rows())
			//		{
			//			rowCount++;

			//			//start with mask, first column, leftmost
			//			UInt64 flags = 0;
			//			//
			//			var columnBit = mask;

			//			stream.Position = 0;

			//			for (var index = 0; index < reader.ColumnCount; index++)
			//			{
			//				string textValue = (string)record[index];
			//				var colType = reader.ColumnTypes[index];

			//				if (textValue == null)
			//				{
			//					//signal only the null flag as true
			//					flags |= columnBit;
			//				}
			//				else
			//				{
			//					switch (colType)
			//					{
			//						case DbColumnType.String:
			//							bufferWriter.Write(textValue);
			//							break;
			//						case DbColumnType.Char:
			//							char charValue = (char)0;
			//							if (!Char.TryParse(textValue, out charValue))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(charValue);
			//							break;
			//						case DbColumnType.Byte:
			//							byte byteValue = 0;
			//							if (!Byte.TryParse(textValue, out byteValue))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(byteValue);
			//							break;
			//						case DbColumnType.Int16:
			//							Int16 int16Value = 0;
			//							if (!Int16.TryParse(textValue, out int16Value))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(int16Value);
			//							break;
			//						case DbColumnType.Int32:
			//							Int32 int32Value = 0;
			//							if (!Int32.TryParse(textValue, out int32Value))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(int32Value);
			//							break;
			//						case DbColumnType.Single:
			//							float floatValue = 0.0f;
			//							if (!float.TryParse(textValue, out floatValue))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(floatValue);
			//							break;
			//						case DbColumnType.Double:
			//							Double doubleValue = 0.0;
			//							if (!Double.TryParse(textValue, out doubleValue))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(doubleValue);
			//							break;
			//						case DbColumnType.Decimal:
			//							Decimal decimalValue = 0;
			//							if (!Decimal.TryParse(textValue, out decimalValue))
			//							{
			//								throw new ArgumentException($"unable to cast: {textValue} to: {colType}");
			//							}
			//							//write
			//							bufferWriter.Write(decimalValue);
			//							break;
			//						default:
			//							throw new ArgumentException($"unsupported type: {colType}");
			//					}
			//				}
			//				//shift right column Bit until it reaches 0 -the last column rightmost
			//				columnBit >>= 1;
			//			}
			//			if (columnBit != 0)
			//			{
			//				Console.WriteLine("Error on column bit flags");
			//			}
			//			//write true binary record
			//			var flagsBuffer = BitConverter.GetBytes(flags);
			//			writer.Write(flagsBuffer, 0, bytes);

			//			//write non-null records
			//			var recBinary = stream.ToArray();
			//			writer.Write(recBinary, 0, recBinary.Length);
			//		}

			//		//write row count
			//		writer.BaseStream.Position = 0;
			//		writer.Write(rowCount);

			//		Console.WriteLine($"writen {rowCount} row(s)");
			//		reader.Dispose();
			//	}
			//}

			//var binFileInfo = new io.FileInfo(binPath);
			//var percent = ((double)binFileInfo.Length / csvFileInfo.Length);
			//Console.WriteLine("binary file is {0:P2}", percent);

			return true;
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

		static void TestDynamic()
		{
			var values = new Dictionary<string, object>();
			values.Add("Title", "Hello World!");
			values.Add("Text", "My first post");
			values.Add("Tags", new[] { "hello", "world" });
			values.Add("Age", 10);

			var post = new DynamicEntity(values);

			dynamic dynPost = post;
			var text = dynPost.Text;
			var g = dynPost["Text"];
			var age = dynPost["Age"];
			var k = dynPost["lo"];

			var age2 = dynPost.Get<int>("Age");
		}
	}

}
