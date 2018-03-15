using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	public class CsvRecordReader
	{
		public CsvDb Database { get; protected internal set; }

		public CsvRecordReader(CsvDb db)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Databse cannot be null or undefined");
			}
		}

		//has a bug
		public List<string[]> Find(string tableName, string columnName, object key)
		{
			//go to page and find <key,[values]>
			//[values] are the offsets inside csv text file

			var table = Database.Table(tableName);
			if (table == null)
			{
				return null;
			}
			var column = table.Column(columnName);
			if (column == null)
			{
				return null;
			}

			var treeIndex = new CsvDbIndexTreeReader(Database, tableName, columnName);

			var itemsPages = new CsvDbIndexItemsReader(Database, tableName, columnName);

			var offset = column.TreeIndexReader.Find(key);

			CsvDbKeyValue<object> item = column.PageItemReader.Find(offset, key);

			var list = new List<string[]>();

			//go to csv and find it records
			var path = io.Path.Combine(Database.BinaryPath, $"{table.Name}.csv");
			var sb = new StringBuilder();
			//char buffer
			var buffer = new char[1024];
			var index = 0;

			using (var reader = new io.StreamReader(path))
			{
				void FillBuffer()
				{
					reader.Read(buffer, 0, buffer.Length);
					index = 0;
				};
				char ReadChar()
				{
					if (index >= buffer.Length)
					{
						FillBuffer();
					}
					return buffer[index++];
				}

				char PeekChar()
				{
					if (index >= buffer.Length)
					{
						FillBuffer();
					}
					return buffer[index];
				}
				
				foreach (var offs in item.Values)
				{
					var count = table.Columns.Count;
					//record of columns
					var record = new string[count];
					var i = 0;
					//point to record offset
					reader.BaseStream.Position = offs;

					//fill buffer initially
					FillBuffer();

					//read csv record columns
					//read  , => add column
					//		  " => start of string, read until next "
					//					if " and next is "", then "" and continue until next "
					//		  otherwise read until next ,

					sb.Length = 0;
					while (i < count)
					{
						var ch = ReadChar();
						if (ch == ',')
						{
							if (sb.Length > 0)
							{
								//not empty
								record[i] = sb.ToString();
							}
							sb.Length = 0;
							i++;
						}
						else if (ch == '"')
						{
							bool endOfString = false;
							while (!endOfString)
							{
								//read as many as possible
								while ((ch = ReadChar()) != '"')
								{
									sb.Append(ch);
								}
								if (PeekChar() == '"')
								{
									index++;
									sb.Append("\"\"");
								}
								else
								{
									endOfString = true;
								}
							}
							//leave sb with value so next comma will add
						}
						else if ((int)ch <= 32)
						{
							//do nothing, discard spaces before comma, "strings", ...
						}
						else
						{
							//read until ,
							do
							{
								sb.Append(ch);
							} while ((ch = ReadChar()) != ',');

							record[i++] = sb.ToString();
							sb.Length = 0;
						}
					}
					//add new record
					list.Add(record);
				}
			}

			return list;
		}

	}
}
