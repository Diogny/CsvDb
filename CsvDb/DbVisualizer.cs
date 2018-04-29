using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;
using System.Reflection;
using System.Linq.Expressions;

namespace CsvDb
{
	[Flags]
	public enum DbVisualize : int
	{
		None = 0,
		//stops every page
		Paged = 1,
		//for not framed paged show a line under page header
		UnderlineHeader = 2,
		//show a box framed around every page
		Framed = 4,
		//display a column with line numbers
		LineNumbers = 8
	}

	public abstract class DbVisualizer : IDisposable
	{
		public CsvDb Database { get; private set; }

		public DbQuery Query { get; protected set; }

		public DbTable Table { get; }

		public int ColumnCount { get; private set; }

		public int RowCount { get; protected set; }

		public bool Paged { get { return (Options & DbVisualize.Paged) != 0; } }

		public DbVisualize Options { get; private set; }

		private static int _pagesize = 32;
		/// <summary>
		/// get/set the page size of the visualization
		/// </summary>
		public static int PageSize
		{
			get { return _pagesize; }
			set
			{
				if (value > 1)
				{
					_pagesize = value;
				}
			}
		}

		public DbColumnTypeEnum[] ColumnTypes { get; private set; }

		protected internal int[] IndexTranslator { get; private set; }

		public IEnumerable<object[]> Rows()
		{
			if (Query.Columns.IsFunction)
			{
				RowCount = 1;
				//Paged doesn't matter, because always 1 row
				yield return Query.Columns.Header;

				//cast and apply function to collection of values
				var column = Query.Columns.AllColumns.First();
				var valueType = ColumnTypes[column.Meta.Index];

				if (Query.Columns.Function == TokenType.COUNT)
				{
					//Enumerable.Count() returns an int
					valueType = DbColumnTypeEnum.Int32;
				}
				else if (!valueType.IsNumeric())
				{
					throw new ArgumentException($"function {Query.Columns.Function} column type must be numeric");
				}

				MethodInfo method =
					this.GetType().GetMethod(nameof(ExecuteFunction), BindingFlags.Instance | BindingFlags.NonPublic);

				MethodInfo genMethod = method.MakeGenericMethod(Type.GetType($"System.{valueType}"));

				var result = genMethod.Invoke(this, new object[] { valueType });

				yield return new object[] { result };
			}
			else
			{
				var collection = ExecuteCollection();

				//LIMIT
				if (Query.Limit > 0)
				{
					collection = collection.Take(Query.Limit);
				}

				RowCount = 0;

				foreach (var record in ReadRecords(collection))
				{
					RowCount++;

					yield return record;
				}
			}
		}

		public void Display()
		{
			if (Query.Columns.IsFunction)
			{
				var row = Rows().ToList();
				var header = row[0][0].ToString();
				var valueStr = row[1][0].ToString();

				Console.WriteLine(header);
				if (Paged && (Options & DbVisualize.UnderlineHeader) != 0)
				{
					Console.WriteLine(new String('─', Math.Max(valueStr.Length, header.Length)));
				}
				Console.WriteLine(valueStr);
			}
			else
			{
				//format anyways with Pagesize for better visibility

				var stop = false;
				var showWait = Paged;
				var headerColumns = Query.Columns.Header;
				//page holder
				var page = new List<List<string>>(PageSize);
				var pageCount = 0;

				var boxed = Paged && (Options & DbVisualize.Framed) != 0;

				var lineNumbers = (Options & DbVisualize.LineNumbers) != 0;

				var enumerator = Rows().GetEnumerator();
				while (!stop)
				{
					//read page
					var count = PageSize;
					page.Clear();

					//format header and column width
					var headerWidths = Query.Columns.Header.Select(h => h.Length).ToList();

					if (lineNumbers)
					{
						//insert minimum of 3 chars for line numbers
						headerWidths.Insert(0, 3);
					}

					while (count-- > 0 && enumerator.MoveNext())
					{
						//convert to string for visualization
						var row = enumerator.Current.Select(c => (c == null) ? String.Empty : c.ToString()).ToList();

						if (lineNumbers)
						{
							row.Insert(0, (RowCount).ToString());
						}

						//add row to page
						page.Add(row);

						//format row columns
						var columnCount = row.Count;
						for (var i = 0; i < columnCount; i++)
						{
							var col = row[i];

							headerWidths[i] = Math.Max(headerWidths[i], (col == null) ? 0 : col.ToString().Length);
						}
					}
					//
					if (!(stop = page.Count == 0))
					{
						var charB = boxed ? '│' : ' ';
						var prefisufix = boxed ? "│" : "";
						var skip = lineNumbers ? 1 : 0;

						//show header
						var header = String.Join(charB,
							Query.Columns.Header.Select((s, ndx) => $" {s.PadRight(headerWidths[ndx + skip])} "));

						if (lineNumbers)
						{
							header = $" {"#".PadRight(headerWidths[0], ' ')} {charB}{header}";
						}

						if (boxed)
						{
							Console.WriteLine($"┌{String.Join('┬', headerWidths.Select(w => new String('─', w + 2)))}┐");
						}

						if (Paged)
						{
							Console.WriteLine($"{prefisufix}{header}{prefisufix}");

							if ((Options & DbVisualize.UnderlineHeader) != 0 && !boxed)
							{
								Console.WriteLine(new String('─', header.Length));
							}
						}

						if (boxed)
						{
							Console.WriteLine($"├{String.Join('┼', headerWidths.Select(w => new String('─', w + 2)))}┤");
						}

						//show columns
						page.ForEach(p =>
						{
							var column = String.Join(charB,
								p.Select((s, ndx) => $" {s.PadRight(headerWidths[ndx])} "));

							Console.WriteLine($"{prefisufix}{column}{prefisufix}");
						});

						if (boxed)
						{
							Console.WriteLine($"└{String.Join('┴', headerWidths.Select(w => new String('─', w + 2)))}┘");
						}

						//if show wait and not first page
						if (showWait && pageCount > 0)
						{
							Console.Write("press any key...");
							var keyCode = Console.ReadKey();
							//remove text
							Console.CursorLeft = 0;
							if (keyCode.Key == ConsoleKey.Escape)
							{
								showWait = false;
							}
						}

						pageCount++;
					}
				}

			}
			Console.WriteLine($" displayed {RowCount} row(s)");
		}

		protected virtual IEnumerable<object[]> ReadRecords(IEnumerable<Int32> offsetCollection)
		{
			yield break;
		}

		public abstract bool IsValid(CsvDb db);

		protected internal string Path = String.Empty;

		protected internal string Extension = String.Empty;

		/// <summary>
		/// Abstract base type of db visualizer, must be disposed to release resources
		/// </summary>
		/// <param name="query"></param>
		/// <param name="extension"></param>
		internal DbVisualizer(CsvDb db, DbQuery query, string extension, DbVisualize options)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Database cannot be null or undefined");
			}
			if ((Query = query) == null)
			{
				throw new ArgumentException("Query cannot be null or undefined");
			}
			if (query.From.Count > 1)
			{
				throw new ArgumentException($"Cannot handle more than one table on from yet!!!!!!!!!");
			}
			if ((Table = db.Table(query.From[0].Name)) == null)
			{
				throw new ArgumentException($"Cannot find table in database");
			}

			//default to CSV
			Extension = String.IsNullOrWhiteSpace(extension) ? CsvDb.SchemaTableDefaultExtension : extension.Trim();

			//path to file
			Path = io.Path.Combine(Database.BinaryPath, $"{Table.Name}.{Extension}");

			//all column types
			ColumnTypes = Table.Columns
				.Select(c => Enum.Parse<DbColumnTypeEnum>(c.Type)).ToArray();

			//save total column count
			ColumnCount = Table.Columns.Count;

			//index translator to real visualized columns
			IndexTranslator = Query.Columns.AllColumns.Select(c => c.Meta.Index).ToArray();

			//dummy
			RowCount = 0;
			Options = options;
		}

		public abstract void Dispose();

		public static DbVisualizer Create(CsvDb db, DbQuery query, DbVisualize options = DbVisualize.Paged)
		{
			if (db == null || query == null)
			{
				return null;
			}
			if ((db.Flags & DbSchemaConfigEnum.Binary) != 0)
			{
				return new DbBinVisualizer(db, query, options);
			}
			else
			{
				return new DbCsvVisualizer(db, query, options);
			}
		}

		#region Execution

		internal T ExecuteFunction<T>(DbColumnTypeEnum valueType)
		{
			//apply quantifiers here

			var offsetResultCollection = ExecuteCollection();

			if(Query.Limit > 0)
			{
				offsetResultCollection = offsetResultCollection.Take(Query.Limit);
			}

			dynamic collection = offsetResultCollection;

			if (Query.Columns.Function != TokenType.COUNT)
			{
				//get real values
				var recordValues = ReadRecords(offsetResultCollection);
				collection = recordValues.Select(cols => cols[0]).Cast<T>();
			}

			MethodInfo method =
				this.GetType().GetMethod(nameof(ApplyFunction), BindingFlags.Instance | BindingFlags.NonPublic);

			MethodInfo genMethod = method.MakeGenericMethod(Type.GetType($"System.{valueType}"));

			var result = genMethod.Invoke(this, new object[] { collection, Query.Columns.Function });

			return (T)result;
		}

		Expression<Func<IEnumerable<T>, T>> CreateLambda<T>(string function)
		{
			var source = Expression.Parameter(
					typeof(IEnumerable<T>), "source");

			var p = Expression.Parameter(typeof(T), "p");

			MethodCallExpression call = Expression.Call(
						typeof(Enumerable), function, new Type[] { typeof(T) }, source);

			return Expression.Lambda<Func<IEnumerable<T>, T>>(call, source);
		}

		internal T ApplyFunction<T>(IEnumerable<T> valueCollection, TokenType function)
		{
			switch (function)
			{
				case TokenType.COUNT:
					var count = valueCollection.Count();
					return (T)Convert.ChangeType(count, typeof(T));
				case TokenType.AVG:
					var avg = valueCollection.Average(x => (dynamic)x);
					return (T)Convert.ChangeType(avg, typeof(T));
				case TokenType.SUM:
					var sum = valueCollection.Sum(x => (dynamic)x);
					return (T)Convert.ChangeType(sum, typeof(T));
				default:
					throw new ArgumentException($"invalid quantifier {function} in query");
			}
			//var lambda = CreateLambda<T>(functionName);
			//var result = lambda.Compile().Invoke(valueCollection);

			//return result;
		}

		internal IEnumerable<int> ExecuteCollection()
		{
			//execute where expresion containing the indexed column
			//	 returns a collection of offset:int
			//with that result
			//  according to AND/OR filter by offset values

			//execute all indexers and stack with logicals
			var oper = TokenType.None;

			IEnumerable<int> result = null;

			var executerClassType = typeof(DbQueryExecuter<>);

			if (Query.Where.Count == 0)
			{
				//find key column
				var table = Database.Table(Query.From[0].Name);
				var keyColumn = table.Columns.FirstOrDefault(c => c.Key);

				if (keyColumn == null)
				{
					throw new ArgumentException($"Cannot find key from table [{table.Name}]");
				}

				var typeIndex = Type.GetType($"System.{keyColumn.Type}");

				Type genericClass = executerClassType.MakeGenericType(typeIndex);

				var objClass = Activator.CreateInstance(genericClass, new object[] {
							keyColumn
						});

				MethodInfo execute_Method = genericClass.GetMethod(nameof(DbQueryExecuter<int>.Execute));

				result = (IEnumerable<int>)execute_Method.Invoke(objClass, new object[] { });
			}
			else
			{
				foreach (var item in Query.Where)
				{
					switch (item.Type)
					{
						case DbQuery.ExpressionEnum.Expression:
							var expr = item as DbQuery.Expression;

							if (expr.TwoColumns)
							{
								throw new ArgumentException($"Two column comparison not supported yet");
							}

							var column = expr.Column;

							var typeIndex = Type.GetType($"System.{column.Type}");

							Type genericClass = executerClassType.MakeGenericType(typeIndex);

							var objClass = Activator.CreateInstance(genericClass, new object[] {
								expr,
								Database
							});

							MethodInfo execute_Method = genericClass.GetMethod("Execute");

							var collection = (IEnumerable<int>)execute_Method.Invoke(objClass, new object[] { });

							if (result == null)
							{
								result = collection;
							}
							else
							{
								//perform logicals
								switch (oper)
								{
									case TokenType.AND:
										result = result.Intersect(collection);
										break;
									case TokenType.OR:
										result = result.Union(collection);
										break;
								}
							}
							break;
						case DbQuery.ExpressionEnum.Logical:
							var logical = item as DbQuery.LogicalExpression;
							oper = logical.Logical;
							break;
					}
				}
			}

			foreach (var offset in result)
			{
				yield return offset;
			}

		}

		#endregion

	}

	public class DbBinVisualizer : DbVisualizer
	{
		public UInt64 Mask { get; private set; }

		protected internal io.BinaryReader reader = null;

		internal DbBinVisualizer(CsvDb db, DbQuery query, DbVisualize options)
			: base(db, query, $"{CsvDb.SchemaTableDataExtension}", options)
		{
			//open reader
			reader = new io.BinaryReader(io.File.OpenRead(Path));
		}

		protected override IEnumerable<object[]> ReadRecords(IEnumerable<Int32> offsetCollection)
		{
			var mainMask = Table.RowMask;
			var bytes = Table.RowMaskLength;

			foreach (var offs in offsetCollection)
			{
				//point to record offset
				reader.BaseStream.Position = offs;

				//read record 
				var buffer = new byte[sizeof(UInt64)];
				reader.Read(buffer, 0, bytes);
				var recordMask = BitConverter.ToUInt64(buffer, 0);
				//copy main
				var bitMask = mainMask;

				var dbRecord = new object[ColumnCount];

				for (var i = 0; i < ColumnCount; i++)
				{
					var colType = ColumnTypes[i];

					if ((recordMask & bitMask) == 0)
					{
						//not null
						switch (colType)
						{
							case DbColumnTypeEnum.Char:
								dbRecord[i] = reader.ReadChar();
								break;
							case DbColumnTypeEnum.Byte:
								dbRecord[i] = reader.ReadByte();
								break;
							case DbColumnTypeEnum.Int16:
								dbRecord[i] = reader.ReadInt16();
								break;
							case DbColumnTypeEnum.Int32:
								dbRecord[i] = reader.ReadInt32();
								break;
							case DbColumnTypeEnum.Int64:
								dbRecord[i] = reader.ReadInt64();
								break;
							case DbColumnTypeEnum.Float:
								dbRecord[i] = reader.ReadSingle();
								break;
							case DbColumnTypeEnum.Double:
								dbRecord[i] = reader.ReadDouble();
								break;
							case DbColumnTypeEnum.Decimal:
								dbRecord[i] = reader.ReadDecimal();
								break;
							case DbColumnTypeEnum.String:
								dbRecord[i] = reader.ReadString();
								break;
						}
					}
					bitMask >>= 1;
				}
				if (!Query.Columns.FullColumns)
				{
					//translate to real
					var record = new object[IndexTranslator.Length];
					for (var i = 0; i < IndexTranslator.Length; i++)
					{
						record[i] = dbRecord[IndexTranslator[i]];
					}
					yield return record;
				}
				else
				{
					//return record
					yield return dbRecord;
				}
			}
		}

		public override bool IsValid(CsvDb db)
		{
			return (db != null) && (db.Flags & DbSchemaConfigEnum.Binary) != 0;
		}

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
			}
		}

	}

	public class DbCsvVisualizer : DbVisualizer
	{
		//char buffer
		char[] buffer = new char[1024];
		int charIndex = 0;
		StringBuilder sb = new StringBuilder();

		protected internal io.StreamReader reader = null;

		void FillBuffer()
		{
			reader.Read(buffer, 0, buffer.Length);
			charIndex = 0;
		}

		char ReadChar()
		{
			if (charIndex >= buffer.Length)
			{
				FillBuffer();
			}
			return buffer[charIndex++];
		}

		char PeekChar()
		{
			if (charIndex >= buffer.Length)
			{
				FillBuffer();
			}
			return buffer[charIndex];
		}

		internal DbCsvVisualizer(CsvDb db, DbQuery query, DbVisualize options)
			: base(db, query, $"{CsvDb.SchemaTableDefaultExtension}", options)
		{
			//open reader
			reader = new io.StreamReader(Path);
		}

		public string[] ReadCsVRecord()
		{
			if (reader.EndOfStream)
			{
				return null;
			}
			var count = ColumnCount;
			var dbRecord = new string[count];
			var columnIndex = 0;

			sb.Length = 0;
			char ch = (char)0;

			//fill buffer initially
			FillBuffer();

			while (columnIndex < count)
			{
				ch = ReadChar();
				//add when comma, (13) \r (10) \n
				switch (ch)
				{
					case ',':
						if (sb.Length > 0)
						{
							//store column
							dbRecord[columnIndex] = sb.ToString();
							sb.Length = 0;
						}
						//point to next column
						columnIndex++;
						break;
					case '\r':
					case '\n':
						if (sb.Length > 0)
						{
							//store column
							dbRecord[columnIndex] = sb.ToString();
							sb.Length = 0;
						}
						//signal end of record
						columnIndex = count;
						break;
					case '"':
						bool endOfString = false;
						while (!endOfString)
						{
							//read as many as possible
							while ((ch = ReadChar()) != '"')
							{
								sb.Append(ch);
							}
							//we got a "
							if (PeekChar() == '"')
							{
								//it's ""
								charIndex++;
								sb.Append("\"\"");
							}
							else
							{
								//consume last "
								//ch = ReadChar();
								//end of string reached
								endOfString = true;
							}
						}
						//leave string in sb until next comma or \r \n
						//this way spaces after "string with spaces" will be discarded
						break;
					default:
						if ((int)ch <= 32)
						{
							//do nothing, discard spaces before comma, "strings", ...
						}
						else
						{
							//read while > ' '
							//single string word or symbol, number, etc
							do
							{
								sb.Append(ch);
							} while ((int)(ch = ReadChar()) > 32 && (ch != ','));
							//read again the ch
							charIndex--;
							//leave string in sb
						}
						break;
				}
			}

			if (!Query.Columns.FullColumns)
			{
				//translate to real
				var record = new string[IndexTranslator.Length];
				for (var i = 0; i < IndexTranslator.Length; i++)
				{
					record[i] = dbRecord[IndexTranslator[i]];
				}
				return record;
			}
			else
			{
				//return record
				return dbRecord;
			}
		}

		protected override IEnumerable<object[]> ReadRecords(IEnumerable<Int32> offsetCollection)
		{
			foreach (var offs in offsetCollection)
			{
				//point to record offset
				reader.BaseStream.Position = offs;

				var record = ReadCsVRecord();

				//record
				yield return record;
			}
		}

		public override bool IsValid(CsvDb db)
		{
			return (db != null) && (db.Flags & DbSchemaConfigEnum.Csv) != 0;
		}

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
			}
		}
	}
}
