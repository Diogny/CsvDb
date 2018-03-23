using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Collections;

namespace CsvDb
{
	public class CsvDbIndexTree<T>
		where T : IComparable<T>
	{
		public CsvDb Database { get { return Index.Table.Database; } }

		public CsvDbColumn Index { get; protected internal set; }

		public PageIndexTreeHeader Header { get; protected internal set; }

		public bool IsUnique { get; protected internal set; }

		public bool IsKey { get; protected internal set; }

		public bool IsLeaf { get; protected internal set; }

		public CsvDbColumnTypeEnum KeyType { get; protected internal set; }

		io.BinaryReader reader = null;

		public PageIndexNodeBase<T> Root { get; protected internal set; }

		public CsvDbIndexTree(CsvDb db, string tableName, string columnName)
		{
			if ((db) == null)
			{
				throw new ArgumentException("Database is undefined");
			}
			Index = db.Index(tableName, columnName);
			if (Index == null)
			{
				throw new ArgumentException($"Column [{columnName}] does not exists in table [{tableName}].");
			}
			//load structure
			var pathTree = io.Path.Combine(db.BinaryPath, $"{Index.Indexer}");
			try
			{
				reader = new io.BinaryReader(io.File.OpenRead(pathTree));

				var headerBuffer = new byte[PageIndexTreeHeader.Size];
				//
				var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
				Header = PageIndexTreeHeader.FromArray(headerBuffer);

				IsUnique = (Header.Flags & Consts.IndexHeaderIsUnique) != 0;
				IsKey = (Header.Flags & Consts.IndexHeaderIsKey) != 0;
				IsLeaf = (Header.Flags & Consts.IndexHeaderIsLeaf) != 0;

				byte keyTypeValue = (byte)Header.Flags;
				KeyType = (CsvDbColumnTypeEnum)keyTypeValue;

				//amount of tree node pages
				Index.NodePages = 0;

				if (!IsLeaf)
				{
					Root = ReadTreePageStructure(0);
				}
			}
			catch (Exception ex)
			{
				throw new ArgumentException($"Could not find indexer [{tableName}].{columnName} in database");
			}
		}

		//later avoid recursive
		PageIndexNodeBase<T> ReadTreePageStructure(int parent)
		{
			//first read the Int32 flag record
			var flags = reader.ReadInt32();
			//
			if (flags == 0)
			{
				//signal tree left or right is null
				return null;
			}

			var thisPageNo = ++Index.NodePages;

			var pageType = flags & 0b011;

			switch (pageType)
			{
				case Consts.BTreePageNodeFlag:
					//read root
					var page = new PageIndexNode<T>(flags, thisPageNo, reader, KeyType);

					page.Left = ReadTreePageStructure(thisPageNo);

					page.Right = ReadTreePageStructure(thisPageNo);

					return page;
				case Consts.BTreePageNodeItemsFlag:
					return new PageIndexItems<T>(flags, thisPageNo, reader);
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

		internal PageIndexNodeBase<T> FindKey(T key)
		{
			PageIndexNodeBase<T> root = Root;

			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						//if not inside items page, then not found
						return root;
					case MetaIndexType.Node:
						//
						var nodePage = root as PageIndexNode<T>;
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

	public enum MetaIndexType
	{
		Node,
		Items
	}

	public interface IBuffered
	{
		byte[] ToByteArray();
	}

	//[StructLayout(LayoutKind.Sequential)]
	public struct PageIndexTreeHeader : IBuffered
	{
		public Int32 Value0;

		public Int32 PageCount;

		public Int32 ColumnIndex;

		public Int32 Flags;

		public static int Size => 16;

		public byte[] ToByteArray()
		{
			var stream = new io.MemoryStream();
			var writer = new io.BinaryWriter(stream);

			writer.Write(this.Value0);
			writer.Write(this.PageCount);
			writer.Write(this.ColumnIndex);
			writer.Write(this.Flags);

			return stream.ToArray();
		}

		public static PageIndexTreeHeader FromArray(byte[] bytes)
		{
			var reader = new io.BinaryReader(new io.MemoryStream(bytes));

			var s = default(PageIndexTreeHeader);

			s.Value0 = reader.ReadInt32();
			s.PageCount = reader.ReadInt32();
			s.ColumnIndex = reader.ReadInt32();
			s.Flags = reader.ReadInt32();

			return s;
		}

	}

	public abstract class PageIndexNodeBase<T> : IEnumerable<PageIndexNodeBase<T>>
		where T : IComparable<T>
	{
		public int Flags { get; private set; }

		public int Number { get; private set; }

		public abstract MetaIndexType Type { get; }

		public abstract IEnumerator<PageIndexNodeBase<T>> GetEnumerator();

		IEnumerator IEnumerable.GetEnumerator() { return this.GetEnumerator(); }

		internal PageIndexNodeBase(int flags, int number)
		{
			Flags = flags;
			Number = number;
		}

	}

	public sealed class PageIndexNode<T> : PageIndexNodeBase<T>
		where T : IComparable<T>
	{
		public int PageSize { get; private set; }

		public bool UniqueValue { get; private set; }

		//later make it generic (internally)
		public T Key { get; private set; }

		public List<int> Values { get; private set; }

		public PageIndexNodeBase<T> Left { get; internal set; }

		public PageIndexNodeBase<T> Right { get; internal set; }

		public override MetaIndexType Type => MetaIndexType.Node;

		internal PageIndexNode(int flags, int number, io.BinaryReader reader, CsvDbColumnTypeEnum keyType)
			: base(flags, number)
		{
			//read info
			PageSize = reader.ReadInt32();
			UniqueValue = (flags & Consts.BTreeUniqueKeyValueFlag) != 0;

			//for Unique = true, this's the unique value, otehrwise it's the Value count
			Int32 keyValueCount = reader.ReadInt32();

			Values = new List<int>();

			//read value(s)
			if (UniqueValue)
			{
				//store unique value
				Values.Add(keyValueCount);
			}
			else
			{
				for (var i = 0; i < keyValueCount; i++)
				{
					Values.Add(reader.ReadInt32());
				}
			}

			//read key, is an object
			Key = (T)keyType.LoadKey(reader);
		}

		public override string ToString() => $"Key: {Key} ({Values.Count}) value(s)";

		//In-Order
		public override IEnumerator<PageIndexNodeBase<T>> GetEnumerator()
		{
			var stack = new Stack<PageIndexNodeBase<T>>();

			//set current to root
			PageIndexNodeBase<T> current = this;

			while (stack.Count > 0 || current != null)
			{
				if (current != null)
				{
					stack.Push(current);
					//try to go Left
					if (current.Type == MetaIndexType.Items)
					{
						current = null;
					}
					else
					{
						current = ((PageIndexNode<T>)current).Left;
					}
				}
				else
				{
					current = stack.Pop();

					//return
					yield return current;

					//try to go Right
					if (current.Type == MetaIndexType.Items)
					{
						current = null;
					}
					else
					{
						current = ((PageIndexNode<T>)current).Right;
					}
				}
				var node = stack.Pop();

			}
		}
	}

	public sealed class PageIndexItems<T> : PageIndexNodeBase<T>
		where T : IComparable<T>
	{
		public int Offset { get; internal set; }

		//should store the amount of items inside this page item

		public override MetaIndexType Type => MetaIndexType.Items;

		internal PageIndexItems(int flags, int number, io.BinaryReader reader)
			: base(flags, number)
		{
			//read info
			Offset = reader.ReadInt32();
		}

		public override string ToString() => $"Offset: {Offset}";

		public override IEnumerator<PageIndexNodeBase<T>> GetEnumerator()
		{
			yield return this;
		}
	}
}
