using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Linq;

namespace CsvDb
{

	/// <summary>
	/// parallel class to develop multi-table.column handling in WHERE
	/// </summary>
	public class DbQueryHandler : IDisposable
	{
		/// <summary>
		/// database
		/// </summary>
		public CsvDb Database { get; private set; }

		/// <summary>
		/// parsed sql query
		/// </summary>
		public DbQuery Query { get; protected set; }

		/// <summary>
		/// contains a collection of unique compiled columns of the sql query
		/// </summary>
		public Dictionary<string, DbColumnHandler> ColumnHandlers { get; }

		/// <summary>
		/// database table data reader collection
		/// </summary>
		public List<DbTableDataReader> TableReaders { get; }

		List<IndexTransf> IndexTranslators;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="db"></param>
		/// <param name="query"></param>
		public DbQueryHandler(CsvDb db, DbQuery query)
		{
			if ((Database = db) == null ||
				(Query = query) == null)
			{
				throw new ArgumentException("Cannot create query executer handler");
			}

			ColumnHandlers = new Dictionary<string, DbColumnHandler>();
			TableReaders = new List<DbTableDataReader>();

			//only where expression table.columns matters here
			foreach (var column in query.Where.Columns)
			{
				var hash = $"{column.Meta.TableName}.{column.Name}";
				if (!ColumnHandlers.ContainsKey(hash))
				{
					ColumnHandlers.Add(hash, new DbColumnHandler(column, db));
					//if new column, then check for new table here too
					if (!TableReaders.Any(t => t.Table.Name == column.Meta.TableName))
					{
						TableReaders.Add(DbTableDataReader.Create(db, column.Meta.TableName));
					}
				}
			}

			//get table data reader from FROM tables too, in case WHERE has no column
			foreach (var table in query.From)
			{
				if (!TableReaders.Any(t => t.Table.Name == table.Name))
				{
					TableReaders.Add(DbTableDataReader.Create(db, table.Name));
				}
			}

			//set index transformers to table columns
			IndexTranslators = query.Columns.Columns.Select(col => new IndexTransf()
			{
				index = col.Meta.Index,
				column = col
			}).ToList();

		}

		public void Dispose()
		{
			foreach (var dbreader in TableReaders)
			{
				dbreader.Dispose();
			}
			TableReaders.Clear();
		}
	}

	struct IndexTransf
	{
		public DbQuery.Column column;

		public int index;

		public override string ToString() => $"[{index} {column}";
	}

	public class DbColumnHandler : IEquatable<DbColumnHandler>
	{
		/// <summary>
		/// the text query column
		/// </summary>
		public DbQuery.Column QueryColumn { get; }

		/// <summary>
		/// real database column
		/// </summary>
		public DbColumn Column { get; }

		public Type Type { get; }

		/// <summary>
		/// holds the current row/record for expression comparison
		/// </summary>
		public object[] Data { get; internal set; }

		/// <summary>
		/// true if it has data
		/// </summary>
		public bool HasData => Data != null;

		public DbColumnHandler(DbQuery.Column column, CsvDb db)
		{
			if ((QueryColumn = column) == null ||
				db == null ||
				(Column = db.Index(column.Meta.TableName, column.Name)) == null ||
				(Type = Type.GetType($"System.{Column.Type}")) == null)
			{
				throw new ArgumentException("compiled column null, empty or no enough data to compile table column");
			}
			//redundant
			Data = null;
		}

		/// <summary>
		/// used for fast retrieving
		/// </summary>
		public string Hash => $"{Column.Table.Name}.{Column.Name}";

		public override int GetHashCode() => Hash.GetHashCode();

		public override bool Equals(object obj)
		{
			return Equals(obj as DbColumnHandler);
		}

		public bool Equals(DbColumnHandler other)
		{
			return other != null && Column.Table.Name == other.Column.Table.Name && Column.Name == other.Column.Name;
		}

		public override string ToString() => Hash;


	}

	/// <summary>
	/// Reads rows from a database table
	/// </summary>
	public abstract class DbTableDataReader : IDisposable
	{
		public abstract void Dispose();

		public DbTable Table { get; }

		internal string Path = String.Empty;

		internal abstract object[] ReadRecord(int offset);

		/// <summary>
		/// column types
		/// </summary>
		public DbColumnType[] ColumnTypes { get; private set; }

		internal DbTableDataReader(DbTable table)
		{
			if ((Table = table) == null)
			{
				throw new ArgumentException("cannot read table data from undefined table");
			}
			ColumnTypes = Table.Columns
				.Select(c => Enum.Parse<DbColumnType>(c.Type)).ToArray();

			//default to CSV
			var extension = table.Database.IsBinary ? CsvDb.SchemaTableDataExtension : CsvDb.SchemaTableDefaultExtension;

			//path to table data file
			Path = io.Path.Combine(table.Database.BinaryPath, $"{Table.Name}.{extension}");
		}

		internal static DbTableDataReader Create(CsvDb db, string tableName)
		{
			if (db == null)
			{
				return null;
			}
			if ((db.Flags & DbSchemaConfigType.Binary) != 0)
			{
				return new DbTableBinDataReader(db.Table(tableName));
			}
			else
			{
				return new DbTableCsvDataReader(db.Table(tableName));
			}
		}

	}

	public class DbTableBinDataReader : DbTableDataReader
	{
		io.BinaryReader reader = null;

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		public UInt64 Mask { get; private set; }

		public DbTableBinDataReader(DbTable table)
			: base(table)
		{
			//open reader
			reader = new io.BinaryReader(io.File.OpenRead(Path));
		}

		internal override object[] ReadRecord(int offset)
		{
			var mainMask = Table.RowMask;
			var bytes = Table.RowMaskLength;

			//point to record offset
			reader.BaseStream.Position = offset;

			//read record 
			var buffer = new byte[sizeof(UInt64)];
			reader.Read(buffer, 0, bytes);
			var recordMask = BitConverter.ToUInt64(buffer, 0);
			//copy main
			var bitMask = mainMask;
			var columnCount = Table.Columns.Count;

			var dbRecord = new object[columnCount];

			for (var i = 0; i < columnCount; i++)
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
			return dbRecord;
		}

	}

	public class DbTableCsvDataReader : DbTableDataReader
	{

		io.StreamReader reader = null;

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		public DbTableCsvDataReader(DbTable table)
			: base(table)
		{
			//open reader
			reader = new io.StreamReader(Path);
		}

		internal override object[] ReadRecord(int offset)
		{
			if (reader.EndOfStream)
			{
				return null;
			}
			var count = Table.Columns.Count;
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

			//return record
			return dbRecord;
		}

		//char buffer
		char[] buffer = new char[1024];
		int charIndex = 0;
		StringBuilder sb = new StringBuilder();

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
	}

}
