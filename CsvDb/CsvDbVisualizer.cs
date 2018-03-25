using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	public class CsvDbVisualizer
	{
		public CsvDbQuery Query { get; protected internal set; }

		public CsvDbVisualizer(CsvDbQuery query)
		{
			if ((Query = query) == null)
			{
				throw new ArgumentException("Query cannot be null or undefined");
			}
		}

		public IEnumerable<string[]> Execute()
		{
			var collection = Query.Execute();

			var path = io.Path.Combine(Query.Database.BinaryPath, $"{Query.TableIdentifier.Table.Name}.csv");

			//go to csv and find it records
			return ReadRecords(path, collection);
		}

		internal List<string[]> ReadRecords(
			string path,
			IEnumerable<Int32> offsetCollection)
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
					var count = Query.TableIdentifier.Table.Columns.Count;
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


	}
}
