using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Linq;

namespace CsvDb
{
	public abstract class DbRecordReader : IDisposable
	{
		public DbTable Table { get; private set; }

		protected internal string Path = String.Empty;

		protected internal string Extension = String.Empty;

		public DbColumnTypeEnum[] ColumnTypes { get; private set; }

		public abstract void Dispose();

		protected abstract IEnumerable<object[]> Read(IEnumerable<Int32> offsetCollection);

		public DbRecordReader(DbTable table)
		{
			if ((Table = table) == null)
			{
				throw new ArgumentException("No database table selected");
			}

			Extension = Table.Database.IsBinary ? CsvDb.SchemaTableDataExtension : CsvDb.SchemaTableDefaultExtension;

			//path to file
			Path = io.Path.Combine(Table.Database.BinaryPath, $"{Table.Name}.{Extension}");

			//all column types
			ColumnTypes = Table.Columns.Select(c => Enum.Parse<DbColumnTypeEnum>(c.Type)).ToArray();
		}

		public static DbRecordReader Create(DbTable table)
		{
			if (table == null)
			{
				return null;
			}
			if (table.Database.IsBinary)
			{
				return new DbBinaryRecordReader(table);
			}
			else
			{
				return new DbTextRecordReader(table);
			}
		}

	}

	public class DbBinaryRecordReader : DbRecordReader
	{

		private io.BinaryReader reader = null;

		internal DbBinaryRecordReader(DbTable table)
			: base(table)
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

		protected override IEnumerable<object[]> Read(IEnumerable<int> offsetCollection)
		{
			var mainMask = Table.RowMask;
			var bytes = Table.RowMaskLength;
			var columnCount = Table.Columns.Count;

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

				var dbRecord = new object[columnCount];

				for (var i = 0; i < columnCount; i++)
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
				//return record
				yield return dbRecord;
			}
		}
	}

	public class DbTextRecordReader : DbRecordReader
	{
		//char buffer
		char[] buffer = new char[1024];
		int charIndex = 0;
		int columnCount = 0;
		StringBuilder sb = new StringBuilder();

		private io.StreamReader reader = null;

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

		internal DbTextRecordReader(DbTable table)
			: base(table)
		{
			//open reader
			reader = new io.StreamReader(Path);
			columnCount = Table.Columns.Count;
		}

		public override void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		protected override IEnumerable<object[]> Read(IEnumerable<int> offsetCollection)
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

		public string[] ReadCsVRecord()
		{
			if (reader.EndOfStream)
			{
				return null;
			}
			var count = columnCount;
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

			//convert record to its object representation
			for (var i = 0; i < columnCount; i++)
			{

			}

			//return record
			return dbRecord;
		}

	}

}
