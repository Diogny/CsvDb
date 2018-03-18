using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;
using System.Reflection;

namespace CsvDb
{
	public class CsvDbIndexTreeReader<T>
		where T : IComparable<T>
	{
		public CsvDb Database { get; protected internal set; }

		public CsvDbColumn Index { get; protected internal set; }

		public CsvDbColumnHeader Header { get; protected internal set; }

		public bool IsUnique { get; protected internal set; }

		public bool IsKey { get; protected internal set; }

		public bool IsLeaf { get; protected internal set; }

		public CsvDbColumnTypeEnum KeyType { get; protected internal set; }

		io.BinaryReader reader = null;

		internal MetaIndexBase<T> Root { get; set; }

		public CsvDbIndexTreeReader(CsvDb db, string tableName, string columnName)
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
			var pathTree = io.Path.Combine(Database.BinaryPath, $"{Index.Indexer}");
			if (!io.File.Exists(pathTree))
			{
				throw new ArgumentException($"Could not find indexer in database");
			}
			using (reader = new io.BinaryReader(io.File.OpenRead(pathTree)))
			{
				var headerBuffer = new byte[CsvDbColumnHeader.Size];
				//
				var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
				Header = CsvDbColumnHeader.FromArray(headerBuffer);

				IsUnique = (Header.Flags & Consts.IndexHeaderIsUnique) != 0;
				IsKey = (Header.Flags & Consts.IndexHeaderIsKey) != 0;
				IsLeaf = (Header.Flags & Consts.IndexHeaderIsLeaf) != 0;

				byte keyTypeValue = (byte)Header.Flags;
				KeyType = (CsvDbColumnTypeEnum)keyTypeValue;

				if (!IsLeaf)
				{
					pageId = 0;
					Root = ReadTreePageStructure(0);
				}
			}

		}

		int pageId = 0;

		MetaIndexBase<T> ReadTreePageStructure(int parent)
		{
			MetaIndexBase<T> page = null;

			var flags = reader.ReadInt32();
			//
			var thisPageNo = ++pageId;

			var pageType = flags & 0b011;
			switch (pageType)
			{
				case Consts.BTreePageNodeFlag:
					var pageSize = reader.ReadInt32();
					var uniqueKeyValue = (flags & Consts.BTreeUniqueKeyValueFlag) != 0;

					Int32 keyValue = 0;
					Int32 keyValueCount = 0;
					var keyValueCollection = new List<int>();

					//read value(s)
					if (uniqueKeyValue)
					{
						keyValue = reader.ReadInt32();
					}
					else
					{
						keyValueCount = reader.ReadInt32();
						for (var i = 0; i < keyValueCount; i++)
						{
							keyValueCollection.Add(reader.ReadInt32());
						}
					}

					//read key
					//is an object
					var key = (T)KeyType.LoadKey(reader);

					page = new MetaIndexNode<T>()
					{
						Number = thisPageNo,
						UniqueValue = uniqueKeyValue,
						Key = key,
						Values = keyValueCollection
					};

					var pageNode = page as MetaIndexNode<T>;

					//left tree page node
					pageNode.Left = ReadTreePageStructure(thisPageNo);

					//right tree page node
					pageNode.Right = ReadTreePageStructure(thisPageNo);

					return page;
				case Consts.BTreePageNodeItemsFlag:
					//
					var offset = reader.ReadInt32();

					page = new MetaIndexItems<T>()
					{
						Number = thisPageNo,
						Offset = offset
					};
					return page;
				default:
					throw new ArgumentException("Invalid database structure!");
			}
		}

		///// <summary>
		///// returns the offset page
		///// </summary>
		///// <param name="key"></param>
		///// <returns></returns>
		//public int Find(object key)
		//{
		//	var keyTypeName = key.GetType().Name;
		//	CsvDbColumnTypeEnum keyType;
		//	if (!Enum.TryParse<CsvDbColumnTypeEnum>(keyTypeName, out keyType) ||
		//		keyType != KeyType)
		//	{
		//		return -2;
		//	}

		//	if (Root == null)
		//	{
		//		//go to .bin file directly, it's ONE page of items
		//		return CsvDbGenerator.ItemsPageStart;
		//	}
		//	else
		//	{
		//		//can be compared here
		//		MetaIndexItems<T> page;
		//		var thisType = this.GetType();
		//		MethodInfo method = thisType
		//				.GetMethod(nameof(FindKey), BindingFlags.Instance | BindingFlags.NonPublic);

		//		Type indexType = Type.GetType($"System.{keyTypeName}");

		//		MethodInfo generic = method.MakeGenericMethod(indexType);

		//		page = generic.Invoke(this,
		//					new object[]
		//					{
		//						Root,
		//						key
		//					}) as MetaIndexItems<T>;

		//		return (page == null) ? -4 : page.Offset;
		//	}
		//}

		internal MetaIndexBase<T> FindKey(T key)
		{
			MetaIndexBase<T> root = Root;

			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						//if not inside items page, then not found
						return root;
					case MetaIndexType.Node:
						//
						var nodePage = root as MetaIndexNode<T>;
						var comp = key.CompareTo((T)nodePage.Key);
						if (comp < 0)
						{
							//go down to the left
							root = nodePage.Left;
						}
						else if (comp > 0)
						{
							//go down to the right
							root = nodePage.Right;
						}
						else
						{
							//it's the root key
							return root;
						}
						break;
				}
			}

			return null;
		}

	}

	internal enum MetaIndexType
	{
		Node,
		Items
	}

	internal abstract class MetaIndexBase<T>
		where T : IComparable<T>
	{
		public int Number { get; set; }

		public abstract MetaIndexType Type { get; }

	}

	internal class MetaIndexNode<T> : MetaIndexBase<T>
		where T : IComparable<T>
	{
		public bool UniqueValue { get; set; }

		//later make it generic (internally)
		public T Key { get; set; }

		public List<int> Values { get; set; }

		public MetaIndexBase<T> Left { get; set; }

		public MetaIndexBase<T> Right { get; set; }

		public override MetaIndexType Type => MetaIndexType.Node;

	}

	internal class MetaIndexItems<T> : MetaIndexBase<T>
		where T : IComparable<T>
	{
		public int Offset { get; set; }

		//should store the amount of items inside this page item

		public override MetaIndexType Type => MetaIndexType.Items;

	}
}
