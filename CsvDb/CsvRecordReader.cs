using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	[Obsolete("This is for testings only, will be removed shortly")]
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

		internal static List<string[]> ReadRecords<T>(
			string path,
			CsvDbTable table,
			T key,
			IEnumerable<Int32> offsetCollection)
			where T : IComparable<T>
		{
			var list = new List<string[]>();

			var sb = new StringBuilder();
			//char buffer
			var buffer = new char[1024];
			var charIndex = 0;

			using (var reader = new io.StreamReader(path))
			{
				void FillBuffer()
				{
					reader.Read(buffer, 0, buffer.Length);
					charIndex = 0;
				};
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
				foreach (var offs in offsetCollection)
				{
					var count = table.Columns.Count;
					//record of columns
					var record = new string[count];
					var columnIndex = 0;
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
					char ch = (char)0;

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
					//add new record
					list.Add(record);
				}
			}
			return list;
		}

		protected internal List<string[]> FindGeneric<T>(CsvDbTable table, CsvDbColumn column, T key)
			where T : IComparable<T>
		{
			//go to page and find <key,[values]>
			//[values] are the offsets inside csv text file

			var treeReader = column.TreeIndexReader<T>();
			int offset = -1;
			if (treeReader.Root == null)
			{
				//go to .bin file directly, it's ONE page of items
				offset = CsvDbGenerator.ItemsPageStart;
			}
			else
			{
				var meta = treeReader.FindKey(key) as MetaIndexItems<T>;
				if (meta == null)
				{
					throw new ArgumentException($"Index corrupted");
				}
				offset = meta.Offset;
			}
			if (offset < 0)
			{
				throw new ArgumentException($"Index corrupted");
			}
			//Console.WriteLine($"Offset: {offset}");
			CsvDbKeyValues<T> item = column.PageItemReader<T>().Find(offset, key);

			var path = io.Path.Combine(Database.BinaryPath, $"{table.Name}.csv");

			//go to csv and find it records
			return ReadRecords<T>(path, table, key, item.Values);
		}

		//going to be erased after testings, CsvDbQuery parse table and column already
		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableColumn">table.column</param>
		/// <param name="oper">operator</param>
		/// <param name="key">key to search for</param>
		/// <returns></returns>
		public List<string[]> Find(string tableColumn, string oper, object key)
		{
			string[] splitted = null;
			if (String.IsNullOrWhiteSpace(tableColumn) ||
				(splitted = tableColumn.Split(".", StringSplitOptions.RemoveEmptyEntries)).Length != 2)
			{
				throw new ArgumentException($"Invalid [table].[column] definition in database");
			}
			return Find(splitted[0], splitted[1], oper, key);
		}

		//going to be erased after testings, CsvDbQuery parse table and column already
		public List<string[]> Find(string tableName, string columnName, string oper, object key)
		{
			var table = Database.Table(tableName ?? "");
			if (table == null)
			{
				throw new ArgumentException($"table [{tableName}] doesnot exist in database");
			}
			var column = table.Column(columnName ?? "");
			if (column == null)
			{
				throw new ArgumentException($"column [{tableName}].{columnName} doesnot exist in database");
			}
			return Find(column, oper, key);
		}

		public List<string[]> Find(CsvDbColumn column, string oper, object key)
		{
			var keyTypeName = key.GetType().Name;
			if (keyTypeName != column.Type)
			{
				throw new ArgumentException($"Unable to retrieve key of type: {keyTypeName}");
			}
			switch (oper = ((oper ?? "").Trim()))
			{
				case "=":
					return FindEqual(column, key);
				case ">":
				case ">=":
					return FindGreaterThan(column, oper, key);
				case "<":
				case "<=":
					return FindLessThan(column, oper, key);
			}
			throw new ArgumentException($"Invalid Operator [{oper}]!");
		}

		protected internal List<string[]> FindEqual(CsvDbColumn column, object key)
		{
			//go to page and find <key,[values]>
			//[values] are the offsets inside csv text file

			var keyType = Type.GetType($"System.{column.Type}");
			//generic method
			MethodInfo findgen_mthd =
				this.GetType()
					.GetMethod(nameof(CsvRecordReader.FindGeneric),
					BindingFlags.Instance | BindingFlags.NonPublic);

			//call generic table processing method
			MethodInfo findGen = findgen_mthd.MakeGenericMethod(keyType);
			//invoke
			var list = findGen.Invoke(this, new object[] { column.Table, column, key });

			return (List<string[]>)list;
		}

		protected internal List<string[]> FindGreaterThan(CsvDbColumn column, string oper, object key)
		{
			CsvDbTable table = column.Table;
			bool includeKey = oper == ">=";
			throw new ArgumentException("Operator > not implemented yet!");
		}

		protected internal List<string[]> FindLessThan(CsvDbColumn column, string oper, object key)
		{
			CsvDbTable table = column.Table;
			bool includeKey = oper == "<=";
			throw new ArgumentException("Operator < not implemented yet!");
		}
	}
}
