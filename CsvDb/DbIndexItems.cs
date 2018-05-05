using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	/// <summary>
	/// Database table column collection of items handler
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class DbIndexItems<T>
			where T : IComparable<T>
	{

		public CsvDb Database { get; }

		public DbColumn Index { get; }

		public Int32 PageCount { get; }

		public DbColumnType KeyType { get; }

		public string PathToItems { get; }

		//implement a cache of pages

		io.BinaryReader reader = null;

		public IEnumerable<MetaItemsPage<T>> Pages { get { return Hash.Values; } }

		public Dictionary<int, MetaItemsPage<T>> Hash { get; protected internal set; }

		public MetaItemsPage<T> this[int offset] => Hash.TryGetValue(offset, out MetaItemsPage<T> page) ? page : null;

		public bool ValidPage(int offset) => this[offset] != null;

		public DbIndexItems(CsvDb db, string tableName, string columnName)
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

			Hash = new Dictionary<int, MetaItemsPage<T>>();

			//read main structure of item pages
			using (reader = new io.BinaryReader(io.File.OpenRead(PathToItems)))
			{
				//Header
				PageCount = reader.ReadInt32();

				Int32 keyTypeValue = reader.ReadInt32();
				KeyType = (DbColumnType)keyTypeValue;

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
						Frequency = 0.0,
						Number = pi
					};

					//add to hash dictionary for fast retrieval
					Hash.Add(page.Offset, page);
				}
			}
			//update item page count
			Index.ItemPages = Hash.Count;
		}

		/// <summary>
		/// Finds an key in a page according to its offset
		/// </summary>
		/// <param name="offset">offset (ID) of page</param>
		/// <param name="key">key</param>
		/// <returns></returns>
		public DbKeyValues<T> Find(int offset, T key)
		{
			MetaItemsPage<T> page = this[offset];

			if (page == null)
			{
				return null;
			}

			var pair = page.Items.FirstOrDefault(i => i.Key.Equals(key));

			return (pair.Key == null) ?
				null :
				new DbKeyValues<T>()
				{
					Key = pair.Key,
					Values = pair.Value
				};
		}

		/// <summary>
		/// Used to search by comparison operators inside a given item page
		/// </summary>
		/// <param name="offset">offset (ID) of the item page</param>
		/// <param name="key">key to compare</param>
		/// <param name="oper">comparison operator</param>
		/// <returns></returns>
		public IEnumerable<int> FindByOper(int offset, T key, TokenType oper)
		{
			MetaItemsPage<T> page = this[offset];
			if (page == null)
			{
				yield break;
			}
			else
			{
				IEnumerable<int> collection = Enumerable.Empty<int>();
				switch (oper)
				{
					case TokenType.Equal:
						collection = page.Items.Where(i => i.Key.Equals(key)).SelectMany(i => i.Value);
						break;
					case TokenType.NotEqual:
						collection = page.Items.Where(i => i.Key.CompareTo(key) != 0).SelectMany(i => i.Value);
						break;
					case TokenType.Less:
						collection = page.Items.Where(i => i.Key.CompareTo(key) < 0).SelectMany(i => i.Value);
						break;
					case TokenType.LessOrEqual:
						collection = page.Items.Where(i => i.Key.CompareTo(key) <= 0).SelectMany(i => i.Value);
						break;
					case TokenType.Greater:
						collection = page.Items.Where(i => i.Key.CompareTo(key) > 0).SelectMany(i => i.Value);
						break;
					case TokenType.GreaterOrEqual:
						collection = page.Items.Where(i => i.Key.CompareTo(key) >= 0).SelectMany(i => i.Value);
						break;
					default:
						throw new ArgumentException($"invalid operator: {oper}");
				}
				foreach (var ofs in collection)
				{
					yield return ofs;
				}
			}
		}

		public IEnumerable<int> All(int offset)
		{
			MetaItemsPage<T> page = this[offset];
			if (page == null)
			{
				yield break;
			}
			else
			{
				foreach (var ofs in page.Items.SelectMany(i => i.Value))
				{
					yield return ofs;
				}
			}
		}

	}

	public class MetaItemsPage<T>
		where T : IComparable<T>
	{
		//read file field
		public Int32 Flags { get; internal set; }

		/// <summary>
		/// ItemsPage's ID
		/// </summary>
		public Int32 Offset { get; internal set; }

		public Int32 PageSize { get; internal set; }

		public Int32 ItemsCount { get; internal set; }

		//generated fields
		public bool UniqueKeyValue { get; internal set; }

		internal int DataStart { get; set; }

		//class properties

		public int Number { get; internal set; }

		public DbIndexItems<T> Parent { get; protected internal set; }

		public Double Frequency { get; internal set; }

		List<KeyValuePair<T, List<int>>> _items = null;

		public IEnumerable<KeyValuePair<T, List<int>>> Items
		{
			get
			{
				//later comply with Database.ReadPolicy

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

						if (Parent.KeyType == DbColumnType.String)
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
				}
				//increase frequency
				Frequency += 0.01;
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
