using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	/// <summary>
	/// Reads rows from a database table
	/// </summary>
	public abstract class DbTableDataReader : IDisposable
	{
		public abstract void Dispose();

		/// <summary>
		/// When null, it's only to read pure database records
		/// </summary>
		public DbQueryHandler Handler { get; }

		public DbTable Table { get; }

		internal string Path = String.Empty;

		/// <summary>
		/// reads a SELECT record
		/// </summary>
		/// <param name="offset">offset</param>
		/// <returns></returns>
		public object[] ReadRecord(int offset)
		{
			var dbRecord = ReadDbRecord(offset);

			if (Handler == null)
			{
				return dbRecord;
			}

			//transform to real record, because we have a query handler
			var record = new object[Handler.ColumnCount];

			foreach (var col in Handler.SelectIndexColumns.Columns)
			{
				record[col.Index] = dbRecord[col.ColumnIndex];
			}

			return record;
		}

		/// <summary>
		/// reads a database record
		/// </summary>
		/// <param name="offset">offset</param>
		/// <returns></returns>
		public abstract object[] ReadDbRecord(int offset);

		/// <summary>
		/// returns all offset of all records in table
		/// </summary>
		/// <typeparam name="T">type of key column</typeparam>
		/// <returns></returns>
		internal IEnumerable<int> Rows<T>()
			where T : IComparable<T>
		{
			var keyColumn = Table.Columns.FirstOrDefault(col => col.Key);

			var treeReader = keyColumn.IndexTree<T>();

			if (treeReader.Root == null)
			{
				//itemspage has only one page, no tree root
				var page = keyColumn.IndexItems<T>().Pages.FirstOrDefault();
				if (page != null)
				{
					foreach (var cvsOfs in page.Items.SelectMany(i => i.Value))
					{
						yield return cvsOfs;
					}
				}
			}
			else
			{
				//In-Order return all values
				foreach (var offs in
					treeReader.DumpTreeNodesInOrder(treeReader.Root)
					.SelectMany(pair => pair.Value))
				{
					yield return offs;
				}
			}
		}

		internal DbTableDataReader(DbTable table, DbQueryHandler handler = null)
		{
			Handler = handler;
			if ((Table = table) == null)
			{
				throw new ArgumentException("cannot read table data from undefined table");
			}

			//default to CSV
			var extension = table.Database.IsBinary ? CsvDb.SchemaTableDataExtension : CsvDb.SchemaTableDefaultExtension;

			//path to table data file
			Path = io.Path.Combine(table.Database.BinaryPath, $"{Table.Name}.{extension}");
		}

		/// <summary>
		/// Creates a database table data reader
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="tableName">table name</param>
		/// <param name="handler">query handler</param>
		/// <returns></returns>
		public static DbTableDataReader Create(CsvDb db, string tableName, DbQueryHandler handler = null)
		{
			if (db == null)
			{
				return null;
			}
			if ((db.Flags & DbSchemaConfigType.Binary) != 0)
			{
				return new DbTableBinDataReader(db.Table(tableName), handler);
			}
			else
			{
				return new DbTableCsvDataReader(db.Table(tableName), handler);
			}
		}

	}

	/// <summary>
	/// Implements a database table binary data reader
	/// </summary>
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

		public DbTableBinDataReader(DbTable table, DbQueryHandler handler = null)
			: base(table, handler)
		{
			//open reader
			reader = new io.BinaryReader(io.File.OpenRead(Path));
		}

		public override object[] ReadDbRecord(int offset)
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
			var columnCount = Table.Count;

			//create record according to table columns
			var dbRecord = new object[columnCount];

			for (var i = 0; i < columnCount; i++)
			{
				var colType = Table.ColumnTypes[i];

				if ((recordMask & bitMask) == 0)
				{
					//not null value
					object value = null;

					switch (colType)
					{
						case DbColumnType.Char:
							value = reader.ReadChar();
							break;
						case DbColumnType.Byte:
							value = reader.ReadByte();
							break;
						case DbColumnType.Int16:
							value = reader.ReadInt16();
							break;
						case DbColumnType.Int32:
							value = reader.ReadInt32();
							break;
						case DbColumnType.Int64:
							value = reader.ReadInt64();
							break;
						case DbColumnType.Single:
							value = reader.ReadSingle();
							break;
						case DbColumnType.Double:
							value = reader.ReadDouble();
							break;
						case DbColumnType.Decimal:
							value = reader.ReadDecimal();
							break;
						case DbColumnType.String:
							value = reader.ReadString();
							break;
						default:
							throw new ArgumentException($"invalid column type: {colType}");
					}
					//store value in right spot
					dbRecord[i] = value;
				}
				bitMask >>= 1;
			}
			//
			return dbRecord;
		}

	}

	/// <summary>
	/// Implements a database table csv/text data reader
	/// </summary>
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

		public DbTableCsvDataReader(DbTable table, DbQueryHandler handler = null)
			: base(table, handler)
		{
			//open reader
			reader = new io.StreamReader(Path);
		}

		public override object[] ReadDbRecord(int offset)
		{
			if (reader.EndOfStream)
			{
				return null;
			}
			var count = Table.Count;
			var columnIndex = 0;
			//var column = Columns[columnIndex];

			//create output record according to real SELECT column count
			var dbRecord = new string[count]; //Handler.ColumnCount

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
			//
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
