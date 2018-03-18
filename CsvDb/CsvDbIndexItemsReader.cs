using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	public class CsvDbIndexItemsReader<T>
		where T : IComparable<T>
	{

		public CsvDb Database { get; protected internal set; }

		public CsvDbColumn Index { get; protected internal set; }

		public Int32 PageCount { get; protected internal set; }

		public CsvDbColumnTypeEnum KeyType { get; protected internal set; }

		public string PathToItems { get; protected internal set; }

		//implement a cache of pages

		io.BinaryReader reader = null;

		internal List<MetaItemsPage<T>> Pages = new List<MetaItemsPage<T>>();

		public bool ValidPage(int offset)
		{
			return Pages.Any(p => p.Offset == offset);
		}

		public CsvDbIndexItemsReader(CsvDb db, string tableName, string columnName)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Database is undefined");
			}
			Index = Database.Index(tableName, columnName);
			if (Index == null)
			{
				throw new ArgumentException($"Column [{columnName}] does not exists in table [{tableName}].");
			}
			//load structure
			PathToItems = io.Path.Combine(Database.BinaryPath, $"{Index.Indexer}.bin");
			if (!io.File.Exists(PathToItems))
			{
				throw new ArgumentException($"Could not find indexer in database");
			}

			//read main structure of item pages
			using (reader = new io.BinaryReader(io.File.OpenRead(PathToItems)))
			{
				//Header

				PageCount = reader.ReadInt32();

				Int32 keyTypeValue = reader.ReadInt32();
				KeyType = (CsvDbColumnTypeEnum)keyTypeValue;

				//read all pages main info
				for (var pi = 0; pi < PageCount; pi++)
				{
					var flags = reader.ReadInt32();
					var pageType = flags & 0b011;

					if (pageType != Consts.BTreePageItemsFlag)
					{
						throw new ArgumentException("Invalid indexer");
					}
					var uniqueKeyValue = (flags & Consts.BTreeUniqueKeyValueFlag) != 0;

					var offset = reader.ReadInt32();

					var pageSize = reader.ReadInt32();

					var itemsCount = reader.ReadInt32();

					//skip keys and values
					//sizeof: flags, offset, pageSize, itemsCount
					var sizeOfInt32 = sizeof(Int32);
					var dataStart = 4 * sizeOfInt32;
					var skip = pageSize - dataStart;

					reader.BaseStream.Seek(skip, io.SeekOrigin.Current);

					var page = new MetaItemsPage<T>()
					{
						Flags = flags,
						UniqueKeyValue = uniqueKeyValue,
						Offset = offset,
						PageSize = pageSize,
						ItemsCount = itemsCount,
						DataStart = dataStart,
						Parent = this,
						Frequency = 0.0
					};
					Pages.Add(page);

				}

			}
		}

		public CsvDbKeyValues<T> Find(int offset, T key)
		{
			var page = Pages.FirstOrDefault(p => p.Offset == offset);

			if (page == null)
			{
				return null;
			}
			var pair = page.Items.FirstOrDefault(i => i.Key.Equals(key));
			return (pair.Key == null) ?
				null :
				new CsvDbKeyValues<T>()
				{
					Key = pair.Key,
					Values = pair.Value
				};
		}
	}

	internal class MetaItemsPage<T>
		where T : IComparable<T>
	{
		//readl file field
		public Int32 Flags { get; set; }

		/// <summary>
		/// ItemsPage's ID
		/// </summary>
		public Int32 Offset { get; set; }

		public Int32 PageSize { get; set; }

		public Int32 ItemsCount { get; set; }

		//generated fields
		public bool UniqueKeyValue { get; set; }

		internal int DataStart { get; set; }

		//class properties
		public CsvDbIndexItemsReader<T> Parent { get; protected internal set; }

		public Double Frequency { get; set; }

		List<KeyValuePair<T, List<int>>> _items = null;

		public IEnumerable<KeyValuePair<T, List<int>>> Items
		{
			get
			{
				if (_items == null)
				{
					var list = new List<KeyValuePair<T, List<int>>>();
					using (var reader = new io.BinaryReader(io.File.OpenRead(Parent.PathToItems)))
					{
						//point to page
						// + discard page header -already read
						var offset = Offset + DataStart;
						reader.BaseStream.Seek(offset, io.SeekOrigin.Begin);

						//keys
						var ii = 0;
						KeyValuePair<T, List<int>> pair;

						if (Parent.KeyType == CsvDbColumnTypeEnum.String)
						{
							var keyLengths = new byte[ItemsCount];
							reader.Read(keyLengths, 0, ItemsCount);
							//
							for (ii = 0; ii < ItemsCount; ii++)
							{
								var charArray = new char[keyLengths[ii]];
								reader.Read(charArray, 0, keyLengths[ii]);
								//
								var stringKey = (T)Convert.ChangeType(new String(charArray), typeof(T));
								pair = new KeyValuePair<T, List<int>>(
									stringKey,
									new List<int>());

								list.Add(pair);
							}
						}
						else
						{
							for (ii = 0; ii < ItemsCount; ii++)
							{
								var key = (T)Parent.KeyType.LoadKey(reader);
								pair = new KeyValuePair<T, List<int>>(
									key,
									new List<int>());
								list.Add(pair);
							}
						}


						//values
						if (UniqueKeyValue)
						{
							for (ii = 0; ii < ItemsCount; ii++)
							{
								list[ii].Value.Add(reader.ReadInt32());
							}
						}
						else
						{
							for (ii = 0; ii < ItemsCount; ii++)
							{
								Int16 itemLen = reader.ReadInt16();
								var itemList = list[ii].Value;
								for (var ip = 0; ip < itemLen; ip++)
								{
									itemList.Add(reader.ReadInt32());
								}
							}
						}

					}
					_items = list;
					Frequency++;
				}
				//
				return _items;
			}
		}

		public void ReleaseMemory()
		{
			_items = null;
		}

	}

}
