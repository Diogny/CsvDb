using CsvDb.Query;
using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	public class DbVisualizer : IDisposable
	{
		public DbQuery Query { get; protected internal set; }

		public int ColumnCount { get; private set; }

		//char buffer
		char[] buffer = new char[1024];
		int charIndex = 0;
		StringBuilder sb = new StringBuilder();
		io.StreamReader reader = null;

		string path = String.Empty;

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

		public DbVisualizer(DbQuery query)
		{
			if ((Query = query) == null)
			{
				throw new ArgumentException("Query cannot be null or undefined");
			}
			path = io.Path.Combine(Query.Database.BinaryPath, $"{Query.FromTableIdentifier.Table.Name}.csv");

			reader = new io.StreamReader(path);

			ColumnCount = Query.FromTableIdentifier.Table.Columns.Count;
		}

		public IEnumerable<string[]> Execute()
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
			var record = new string[count];
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
							record[columnIndex] = sb.ToString();
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
							record[columnIndex] = sb.ToString();
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
			return record;
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

		public void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
			}
		}
	}
}
