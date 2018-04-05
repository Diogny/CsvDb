using CsvDb.Query;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	public abstract class DbVisualizer : IDisposable
	{
		public DbQuery Query { get; protected internal set; }

		public int ColumnCount { get; private set; }

		public DbColumnTypeEnum[] ColumnTypes { get; private set; }

		protected internal int[] IndexTranslator { get; private set; }

		public abstract IEnumerable<object[]> Execute();

		public abstract bool IsValid(CsvDb db);

		protected internal string Path = String.Empty;

		protected internal string Extension = String.Empty;

		/// <summary>
		/// Abstract base type of db visualizer, must be disposed to release resources
		/// </summary>
		/// <param name="query"></param>
		/// <param name="extension"></param>
		internal DbVisualizer(DbQuery query, string extension)
		{
			if ((Query = query) == null)
			{
				throw new ArgumentException("Query cannot be null or undefined");
			}
			//default to CSV
			Extension = String.IsNullOrWhiteSpace(extension) ? "csv" : extension.Trim();

			//path to file
			Path = io.Path.Combine(Query.Database.BinaryPath, $"{Query.FromTableIdentifier.Table.Name}.{Extension}");

			//save total column count
			ColumnCount = Query.FromTableIdentifier.Table.Columns.Count;

			//all column types
			ColumnTypes = Query.FromTableIdentifier.Table.Columns
				.Select(c => Enum.Parse<DbColumnTypeEnum>(c.Type)).ToArray();

			//index translator to real visualized columns
			IndexTranslator = Query.Columns.Columns.Select(c => c.Column.Index).ToArray();
		}

		public abstract void Dispose();

		public static DbVisualizer Create(DbQuery query)
		{
			if (query == null)
			{
				return null;
			}
			if ((query.Database.Flags & DbSchemaConfigEnum.Binary) != 0)
			{
				return new DbBinVisualizer(query);
			}
			else
			{
				return new DbCsvVisualizer(query);
			}
		}

	}

	public class DbBinVisualizer : DbVisualizer
	{
		public int RowCount { get; private set; }

		public UInt64 Mask { get; private set; }

		protected internal io.BinaryReader reader = null;

		internal DbBinVisualizer(DbQuery query)
			: base(query, "bin")
		{
			//open reader
			reader = new io.BinaryReader(io.File.OpenRead(Path));
		}

		public override IEnumerable<object[]> Execute()
		{
			var collection = Query.Execute();

			foreach (var record in ReadRecords(collection))
			{
				yield return record;
			}
		}

		IEnumerable<object[]> ReadRecords(IEnumerable<Int32> offsetCollection)
		{
			var mainMask = Query.FromTableIdentifier.Table.RowMask;
			var bytes = Query.FromTableIdentifier.Table.RowMaskLength;

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
				if (!Query.Columns.IsFull)
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

		internal DbCsvVisualizer(DbQuery query)
			: base(query, "csv")
		{
			//open reader
			reader = new io.StreamReader(Path);
		}

		public override IEnumerable<object[]> Execute()
		{
			var collection = Query.Execute();

			//go to csv and find it records
			return ReadOffsetRecords(collection);
		}

		public string[] ReadRecord()
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

			if (!Query.Columns.IsFull)
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

		internal List<string[]> ReadOffsetRecords(IEnumerable<Int32> offsetCollection)
		{
			var list = new List<string[]>();

			foreach (var offs in offsetCollection)
			{
				//point to record offset
				reader.BaseStream.Position = offs;

				var record = ReadRecord();

				//add new record
				list.Add(record);
			}

			return list;
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
