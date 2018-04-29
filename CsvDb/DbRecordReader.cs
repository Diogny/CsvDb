using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Linq;
using System.Reflection;
using System.Linq.Expressions;

namespace CsvDb
{
	/// <summary>
	/// Represents a disposable database record/row reader
	/// </summary>
	public abstract class DbRecordReader : IDisposable
	{
		/// <summary>
		/// disposes reader, must be called to free resources
		/// </summary>
		public abstract void Dispose();

		/// <summary>
		/// database
		/// </summary>
		public CsvDb Database { get; private set; }

		/// <summary>
		/// parsed sql query
		/// </summary>
		public DbQuery Query { get; protected set; }

		public DbTable Table { get; }

		/// <summary>
		/// column count
		/// </summary>
		public int ColumnCount { get; private set; }

		/// <summary>
		/// row/record count
		/// </summary>
		public int RowCount { get; protected set; }

		/// <summary>
		/// column types
		/// </summary>
		public DbColumnType[] ColumnTypes { get; private set; }

		internal int[] IndexTranslator { get; private set; }

		internal abstract IEnumerable<object[]> ReadRecords(IEnumerable<Int32> offsetCollection);

		public abstract bool IsValid(CsvDb db);

		internal string Path = String.Empty;

		internal string Extension = String.Empty;

		/// <summary>
		/// creates an abstract database record/row reader
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="query">parsed sql query</param>
		/// <param name="extension">extension used for data</param>
		internal DbRecordReader(CsvDb db, DbQuery query, string extension)
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
				.Select(c => Enum.Parse<DbColumnType>(c.Type)).ToArray();

			//save total column count
			ColumnCount = Table.Columns.Count;

			//index translator to real visualized columns
			IndexTranslator = Query.Columns.AllColumns.Select(c => c.Meta.Index).ToArray();

			RowCount = 0;
		}

		/// <summary>
		/// returns a collection of all sql query rows
		/// </summary>
		/// <returns></returns>
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
					valueType = DbColumnType.Int32;
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

		/// <summary>
		/// Creates a database record/row reader from a database
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="query">parsed sql query</param>
		/// <returns></returns>
		public static DbRecordReader Create(CsvDb db, DbQuery query)
		{
			if (db == null || query == null)
			{
				return null;
			}
			if ((db.Flags & DbSchemaConfigType.Binary) != 0)
			{
				return new DbBinaryRecordReader(db, query);
			}
			else
			{
				return new DbCsvRecordReader(db, query);
			}
		}

		#region Execution

		internal T ExecuteFunction<T>(DbColumnType valueType)
		{
			//apply quantifiers here

			var offsetResultCollection = ExecuteCollection();

			if (Query.Limit > 0)
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

	/// <summary>
	/// Database binary record/row binary reader
	/// </summary>
	public class DbBinaryRecordReader : DbRecordReader
	{
		public UInt64 Mask { get; private set; }

		io.BinaryReader reader = null;

		internal DbBinaryRecordReader(CsvDb db, DbQuery query)
			: base(db, query, $"{CsvDb.SchemaTableDataExtension}")
		{
			//open reader
			reader = new io.BinaryReader(io.File.OpenRead(Path));
		}

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		public override bool IsValid(CsvDb db) => (db != null) && (db.Flags & DbSchemaConfigType.Binary) != 0;

		internal override IEnumerable<object[]> ReadRecords(IEnumerable<int> offsetCollection)
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
							case DbColumnType.Char:
								dbRecord[i] = reader.ReadChar();
								break;
							case DbColumnType.Byte:
								dbRecord[i] = reader.ReadByte();
								break;
							case DbColumnType.Int16:
								dbRecord[i] = reader.ReadInt16();
								break;
							case DbColumnType.Int32:
								dbRecord[i] = reader.ReadInt32();
								break;
							case DbColumnType.Int64:
								dbRecord[i] = reader.ReadInt64();
								break;
							case DbColumnType.Float:
								dbRecord[i] = reader.ReadSingle();
								break;
							case DbColumnType.Double:
								dbRecord[i] = reader.ReadDouble();
								break;
							case DbColumnType.Decimal:
								dbRecord[i] = reader.ReadDecimal();
								break;
							case DbColumnType.String:
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

	}

	/// <summary>
	/// Database binary record/row text/csv reader
	/// </summary>
	public class DbCsvRecordReader : DbRecordReader
	{
		//char buffer
		char[] buffer = new char[1024];
		int charIndex = 0;
		StringBuilder sb = new StringBuilder();

	  io.StreamReader reader = null;

		internal DbCsvRecordReader(CsvDb db, DbQuery query)
		: base(db, query, $"{CsvDb.SchemaTableDefaultExtension}")
		{
			//open reader
			reader = new io.StreamReader(Path);
		}

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		public override bool IsValid(CsvDb db) => (db != null) && (db.Flags & DbSchemaConfigType.Csv) != 0;

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

		string[] ReadCsVRecord()
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

		internal override IEnumerable<object[]> ReadRecords(IEnumerable<int> offsetCollection)
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
	}

}