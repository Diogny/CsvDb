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

		/// <summary>
		/// Database table
		/// </summary>
		public DbTable Table { get; }

		//path to data file
		internal string Path = String.Empty;

		/// <summary>
		/// Creates a database data table reader
		/// </summary>
		/// <param name="table">table</param>
		/// <param name="handler">query handler</param>
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
			return Create(db, db?.Table(tableName), handler);
		}

		/// <summary>
		/// Creates a database data table reader
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="table">table</param>
		/// <param name="handler">query handler</param>
		/// <returns></returns>
		public static DbTableDataReader Create(CsvDb db, DbTable table, DbQueryHandler handler = null)
		{
			if (db == null || table == null)
			{
				return null;
			}
			if ((db.Flags & DbSchemaConfigType.Binary) != 0)
			{
				return new DbTableBinDataReader(table, handler);
			}
			else
			{
				return new DbTableCsvDataReader(table, handler);
			}
		}

		#region Records

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
		/// returns all offset of all records in table of first key column
		/// </summary>
		/// <typeparam name="T">type of key column</typeparam>
		/// <returns></returns>
		internal IEnumerable<int> Rows<T>(DbColumn column = null)
			where T : IComparable<T>
		{
			if (column == null)
			{
				yield break;
			}

			var treeReader = column.IndexTree<T>();

			if (treeReader.Root == null)
			{
				//itemspage has only one page, no tree root
				var page = column.IndexItems<T>().Pages.FirstOrDefault();
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

		#endregion

		#region Compare & Find

		/// <summary>
		/// Fast indexed column coomparison by a constant value
		/// </summary>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">comlumn to compare</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		internal IEnumerable<KeyValuePair<T, List<int>>> CompareIndexedKeyWithKey<T>(
			DbColumn key, DbColumn column, TokenType comparer, T value)
			where T : IComparable<T>
		{
			var nodeTree = column.IndexTree<T>();
			int offset = -1;

			var collection = Enumerable.Empty<KeyValuePair<T, List<int>>>();

			if (nodeTree.Root == null)
			{
				//go to .bin file directly, it's ONE page of items
				collection = column.IndexItems<T>()
					.FindByOper(DbGenerator.ItemsPageStart, value, comparer);
			}
			else
			{
				//this's for tree node page structure
				switch (comparer)
				{
					case TokenType.Equal: // "="
																//find page with key
						var baseNode = nodeTree.FindKey(value) as PageIndexNodeBase<T>;
						if (baseNode != null)
						{
							switch (baseNode.Type)
							{
								case MetaIndexType.Node:
									//it's in a tree node page
									var nodePage = baseNode as PageIndexNode<T>;
									if (nodePage != null && nodePage.Values != null)
									{
										collection = new KeyValuePair<T, List<int>>[] {
											new KeyValuePair<T,List<int>>(value, nodePage.Values)
										};
									}
									break;
								case MetaIndexType.Items:
									//it's in a items page 
									offset = ((PageIndexItems<T>)baseNode).Offset;
									DbKeyValues<T> item = column.IndexItems<T>().Find(offset, value);
									if (item != null && item.Values != null)
									{
										collection = new KeyValuePair<T, List<int>>[] {
											new KeyValuePair<T,List<int>>(value, item.Values)
										};
									}
									break;
							}
						}
						break;
					case TokenType.Less: // "<"
						collection = nodeTree.FindLessKey(value, TokenType.Less);
						break;
					case TokenType.LessOrEqual: // "<="
						collection = nodeTree.FindLessKey(value, TokenType.LessOrEqual);
						break;
					case TokenType.Greater: // ">"
						collection = nodeTree.FindGreaterKey(value, TokenType.Greater);
						break;
					case TokenType.GreaterOrEqual:  //">="
						collection = nodeTree.FindGreaterKey(value, TokenType.GreaterOrEqual);
						break;
						//throw new ArgumentException($"Operator: {Expression.Operator.Name} not implemented yet!");
				}
			}

			foreach (var ofs in collection)
			{
				yield return ofs;
			}
		}

		/// <summary>
		/// Fast indexed column coomparison by a constant value
		/// </summary>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">comlumn to compare</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		internal IEnumerable<int> CompareIndexedKey<T>(
			DbColumn key, DbColumn column, TokenType comparer, T value)
			where T : IComparable<T>
		{
			return CompareIndexedKeyWithKey<T>(key, column, comparer, value)
				.SelectMany(pair => pair.Value);
		}

		/// <summary>
		/// Expensive non-indexed column comparison by a constant value
		/// </summary>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">column</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		public abstract IEnumerable<int> CompareNonIndexedKey<T>(
				DbColumn key, DbColumn column, TokenType comparer, T value)
			where T : IComparable<T>;

		/// <summary>
		/// Expensive non-indexed column comparison by a constant value
		/// </summary>
		/// <typeparam name="K">type of column key</typeparam>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">column</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns>key value pair with key and offset</returns>
		public abstract IEnumerable<KeyValuePair<K, int>> CompareNonIndexedKeyWithKey<K, T>(
				DbColumn key, DbColumn column, TokenType comparer, T value)
			where K : IComparable<K>
			where T : IComparable<T>;

		/// <summary>
		/// Compare a table column with a constant value. Decide best way for indexed and non-indexed columns.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="column">column</param>
		/// <param name="comparer">comparer operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		public IEnumerable<int> Compare<T>(DbColumn column, TokenType comparer, T value)
			where T : IComparable<T>
		{
			if (column == null || !comparer.IsComparison())
			{
				yield break;
			}
			//get key column
			var keyColumn = column.Table.Columns.FirstOrDefault(c => c.Key);

			var collection = column.Indexed ?
				CompareIndexedKey<T>(keyColumn, column, comparer, value) :
				CompareNonIndexedKey<T>(keyColumn, column, comparer, value);

			foreach (var offset in collection)
			{
				yield return offset;
			}
		}

		#endregion

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

		public int[] ColumnTypeSizes { get; }

		/// <summary>
		/// Creates a database table binary data reader
		/// </summary>
		/// <param name="table">table</param>
		/// <param name="handler">query handler</param>
		public DbTableBinDataReader(DbTable table, DbQueryHandler handler = null)
			: base(table, handler)
		{
			reader = new io.BinaryReader(io.File.OpenRead(Path));
			//build column type translator
			ColumnTypeSizes = Table.ColumnTypes.Select(t => t.GetSize()).ToArray();
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

		/// <summary>
		/// Expensive non-indexed column comparison by a constant value
		/// </summary>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">column to compare</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		public override IEnumerable<int> CompareNonIndexedKey<T>(
			DbColumn key, DbColumn column, TokenType comparer, T value)
		{
			//THIS CANNOT BE TOGETHER WITH CompareNonIndexedKeyWithKey BECAUSE
			// RETURNING KEY MAKE AN EXTRA LOAD

			var keyCount = 0;

			//read indexer offsets in their order
			using (var keyReader =
				new io.BinaryReader(
					io.File.OpenRead($"{key.Table.Database.BinaryPath}\\{key.Hash}.offset")))
			{
				keyCount = keyReader.ReadInt32();

				var count = Table.Count;
				var initialCount = column.Index;

				var buffer = new byte[sizeof(UInt64)];
				var bytes = Table.RowMaskLength;
				var mainMask = Table.RowMask;

				int columnIndex = 0;
				int columnSize = 0;
				int bytesToSkip = 0;
				ulong bitMask = 0;
				ulong recordMask = 0;

				//build column type translator
				var columnTypeSizes = Table.ColumnTypes.Select(t => t.GetSize()).ToArray();

				void SKIP(int cnt)
				{
					while (cnt-- > 0)
					{
						if ((recordMask & bitMask) == 0)
						{
							//not null value, add according to column type
							var size = columnTypeSizes[columnIndex];
							if (size == 0)
							{
								throw new ArgumentException("unsupported type size");
							}
							else if (size > 0)
							{
								bytesToSkip += size;
							}
							else
							{
								//string, read length, and add chars
								reader.BaseStream.Position += bytesToSkip;


								//read new bytes to skip
								bytesToSkip = reader.ReadByte();
							}

						}
						bitMask >>= 1;
						columnSize = ColumnTypeSizes[++columnIndex];
					}
					//position reader
					if (bytesToSkip > 0)
					{
						reader.BaseStream.Position += bytesToSkip;
					}
				}

				T GetColumnValue<T>()
				{
					//for string, read length byte
					var size = (columnSize < 0) ? reader.ReadByte() : columnSize;
					//create buffer
					var columnBuffer = new byte[size];
					//read it
					reader.Read(columnBuffer, 0, size);
					//convert it to generic value
					return columnBuffer.BitConvertTo<T>();
				}

				for (var ndx = 0; ndx < keyCount; ndx++)
				{
					var offset = keyReader.ReadInt32();
					//point to record offset
					reader.BaseStream.Position = offset;

					//copy main mask
					bitMask = mainMask;

					//read record mask
					reader.Read(buffer, 0, bytes);
					recordMask = BitConverter.ToUInt64(buffer, 0);

					//developer only
					var binary = Convert.ToString((long)recordMask, 2);

					columnSize = ColumnTypeSizes[columnIndex = 0];
					bytesToSkip = 0;

					//skip initial columns if any
					if (initialCount > 0)
					{
						SKIP(initialCount);
					}

					//read column value to compare
					var columnValue = GetColumnValue<T>();

					//compare
					if (comparer.Compare<T>(columnValue, value))
					{
						yield return offset;
					}
				}
			}
		}

		public override IEnumerable<KeyValuePair<K, int>> CompareNonIndexedKeyWithKey<K, T>(
			DbColumn key, DbColumn column, TokenType comparer, T value)
		{
			if (column.Indexed || column.Table.Multikey)
			{
				throw new ArgumentException($"Column {column} must be un-indexed, and belong to a One-Key table");
			}

			//get the key column, index
			var keyColumn = column.Table.Key;
			int keyColumnIndex = column.Index;
			K keyValue = default(K);

			var keyCount = 0;

			//read indexer offsets in their order
			using (var keyReader =
				new io.BinaryReader(
					io.File.OpenRead($"{key.Table.Database.BinaryPath}\\{key.Hash}.offset")))
			{
				//amount of keys
				keyCount = keyReader.ReadInt32();

				var count = Table.Count;
				var initialCount = column.Index;

				var buffer = new byte[sizeof(UInt64)];
				var bytes = Table.RowMaskLength;
				var mainMask = Table.RowMask;

				int columnIndex = 0;
				int columnSize = 0;

				int bytesToSkip = 0;
				ulong bitMask = 0;
				ulong recordMask = 0;

				void SKIP(int cnt)
				{
					while (cnt-- > 0)
					{
						if ((recordMask & bitMask) == 0)
						{
							if (columnIndex == keyColumnIndex)
							{
								//read Key value into: keyValue
								keyValue = GetColumnValue<K>();
							}
							else
							{
								//SKIP
								//not null value, add according to column type
								if (columnSize == 0)
								{
									throw new ArgumentException("unsupported type size");
								}
								else if (columnSize > 0)
								{
									bytesToSkip += columnSize;
								}
								else
								{
									//string, read length, and add chars
									reader.BaseStream.Position += bytesToSkip;

									//read new bytes to skip
									bytesToSkip = reader.ReadByte();
								}
							}
						}
						bitMask >>= 1;
						columnSize = ColumnTypeSizes[++columnIndex];
					}
					//position reader
					if (bytesToSkip > 0)
					{
						reader.BaseStream.Position += bytesToSkip;
					}
				}

				C GetColumnValue<C>()
				{
					//for string, read length byte
					var size = (columnSize < 0) ? reader.ReadByte() : columnSize;
					//create buffer
					var columnBuffer = new byte[size];
					//read it
					reader.Read(columnBuffer, 0, size);
					//convert it to generic value
					return columnBuffer.BitConvertTo<C>();
				}

				for (var ndx = 0; ndx < keyCount; ndx++)
				{
					var offset = keyReader.ReadInt32();
					//point to record offset
					reader.BaseStream.Position = offset;

					//copy main mask
					bitMask = mainMask;

					//read record mask
					reader.Read(buffer, 0, bytes);
					recordMask = BitConverter.ToUInt64(buffer, 0);

					//developer only
					var binary = Convert.ToString((long)recordMask, 2);

					//
					columnSize = ColumnTypeSizes[columnIndex = 0];
					bytesToSkip = 0;

					//skip initial columns if any
					if (initialCount > 0)
					{
						SKIP(initialCount);
					}

					//read column value to compare
					var columnValue = GetColumnValue<T>();

					if (comparer.Compare<T>(columnValue, value))
					{
						if (keyColumnIndex > columnIndex)
						{
							//key not read yet
							SKIP(keyColumnIndex - columnIndex - 1);

							//read Key value into: keyValue
							keyValue = GetColumnValue<K>();
						}

						yield return new KeyValuePair<K, int>(keyValue, offset);
					}
				}
			}
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

		/// <summary>
		/// Creates a database table csv/text data reader
		/// </summary>
		/// <param name="table">table</param>
		/// <param name="handler">query handler</param>
		public DbTableCsvDataReader(DbTable table, DbQueryHandler handler = null)
			: base(table, handler)
		{
			reader = new io.StreamReader(Path);
		}

		#region Records

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

		#endregion

		/// <summary>
		/// Expensive non-indexed column comparison by a constant value
		/// </summary>
		/// <typeparam name="T">type of column</typeparam>
		/// <param name="key">table key</param>
		/// <param name="column">column to compare</param>
		/// <param name="comparer">comparison operator</param>
		/// <param name="value">constant value</param>
		/// <returns></returns>
		public override IEnumerable<int> CompareNonIndexedKey<T>(DbColumn key, DbColumn column, TokenType comparer, T value)
		{
			throw new NotImplementedException();
		}

		public override IEnumerable<KeyValuePair<K, int>> CompareNonIndexedKeyWithKey<K, T>(DbColumn key, DbColumn column, TokenType comparer, T value)
		{
			throw new NotImplementedException();
		}
	}
}
