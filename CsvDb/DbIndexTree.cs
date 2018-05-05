using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Collections;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// Database table column node tree handler
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class DbIndexTree<T>
		where T : IComparable<T>
	{
		public CsvDb Database { get { return Index.Table.Database; } }

		public DbColumn Index { get; }

		public PageIndexTreeHeader Header { get; }

		public bool IsUnique { get; }

		public bool IsKey { get; }

		public bool IsLeaf { get; }

		public DbColumnType KeyType { get; }

		io.BinaryReader reader = null;

		public PageIndexNodeBase<T> Root { get; }

		public DbIndexTree(DbColumn index)
		{
			if ((Index = index) == null)
			{
				throw new ArgumentException($"Column indexer cannot be null or error");
			}
			//load structure
			try
			{
				var pathTree = io.Path.Combine(Index.Table.Database.BinaryPath, $"{Index.Indexer}");

				reader = new io.BinaryReader(io.File.OpenRead(pathTree));

				var headerBuffer = new byte[PageIndexTreeHeader.Size];
				//
				var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
				Header = PageIndexTreeHeader.FromArray(headerBuffer);

				IsUnique = (Header.Flags & Consts.IndexHeaderIsUnique) != 0;
				IsKey = (Header.Flags & Consts.IndexHeaderIsKey) != 0;
				IsLeaf = (Header.Flags & Consts.IndexHeaderIsLeaf) != 0;

				byte keyTypeValue = (byte)Header.Flags;
				KeyType = (DbColumnType)keyTypeValue;

				//amount of tree node pages
				Index.NodePages = 0;

				if (!IsLeaf)
				{
					Root = ReadTreePageStructure(0);
				}
			}
			catch
			{
				throw new ArgumentException($"Error processing column indexer {Index.Table.Name}.{Index.Name}");
			}
		}

		public DbIndexTree(CsvDb db, string tableName, string columnName)
			: this(db?.Index(tableName, columnName))
		{ }

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

		/// <summary>
		/// Finds all keys less than an specific key
		/// </summary>
		/// <param name="key">key</param>
		/// <returns></returns>
		internal IEnumerable<int> FindLessThanKey(T key)
		{
			PageIndexNodeBase<T> root = Root;

			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						var itemsPage = root as PageIndexItems<T>;
						foreach (var ofs in Index.IndexItems<T>()
							.FindByOper(itemsPage.Offset, key, TokenType.Less))
						{
							yield return ofs;
						}
						//stop here
						root = null;
						break;
					case MetaIndexType.Node:
						var nodePage = root as PageIndexNode<T>;
						//compare key to node:key
						var comp = key.CompareTo(nodePage.Key);
						if (comp < 0)
						{
							//go down to the left
							root = nodePage.Left;
						}
						else
						{
							//key's greater or equal than node key

							//return all left nodes -in-order
							foreach (var ofs in DumpTreeNodesInOrder(nodePage.Left))
							{
								yield return ofs;
							}

							if (comp > 0)
							{
								//return root values
								foreach (var ofs in nodePage.Values)
								{
									yield return ofs;
								}

								//continue probing right node
								root = nodePage.Right;
							}
							else
							{
								//stop here
								root = null;
							}
						}
						break;
				}
			}
		}

		/// <summary>
		/// Finds all keys less or equal than an specific key
		/// </summary>
		/// <param name="key">key</param>
		/// <returns></returns>
		internal IEnumerable<int> FindLessOrEqualThanKey(T key)
		{
			PageIndexNodeBase<T> root = Root;

			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						var itemsPage = root as PageIndexItems<T>;
						foreach (var ofs in Index.IndexItems<T>()
							.FindByOper(itemsPage.Offset, key, TokenType.LessOrEqual))
						{
							yield return ofs;
						}
						//stop here
						root = null;
						break;
					case MetaIndexType.Node:
						var nodePage = root as PageIndexNode<T>;
						//compare key to node:key
						var comp = key.CompareTo(nodePage.Key);
						if (comp < 0)
						{
							//go down to the left
							root = nodePage.Left;
						}
						else
						{
							//return all left nodes -in-order
							foreach (var ofs in DumpTreeNodesInOrder(nodePage.Left))
							{
								yield return ofs;
							}

							//return root
							foreach (var ofs in nodePage.Values)
							{
								yield return ofs;
							}

							if (comp > 0)
							{
								//continue probing right node
								root = nodePage.Right;
							}
							else
							{
								//stop here
								root = null;
							}
						}
						break;
				}
			}
		}

		/// <summary>
		/// Finds all keys greater than an specific key
		/// </summary>
		/// <param name="key">key</param>
		/// <returns></returns>
		internal IEnumerable<int> FindGreaterThanKey(T key)
		{
			foreach (var ofs in FindGreaterThanKey(Root, key))
			{
				yield return ofs;
			}
		}

		internal IEnumerable<int> FindGreaterThanKey(PageIndexNodeBase<T> root, T key)
		{
			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						var itemsPage = root as PageIndexItems<T>;
						foreach (var ofs in Index.IndexItems<T>()
							.FindByOper(itemsPage.Offset, key, TokenType.Greater))
						{
							yield return ofs;
						}
						//stop here
						root = null;
						break;
					case MetaIndexType.Node:
						var nodePage = root as PageIndexNode<T>;
						//compare key to node:key
						var comp = key.CompareTo(nodePage.Key);
						if (comp > 0)
						{
							//continue probing right node
							root = nodePage.Right;
						}
						else
						{
							if (comp < 0)
							{
								//probe left node
								foreach (var ofs in FindGreaterThanKey(nodePage.Left, key))
								{
									yield return ofs;
								}
							}

							//return root values
							foreach (var ofs in nodePage.Values)
							{
								yield return ofs;
							}

							//return all right nodes -in-order
							foreach (var ofs in DumpTreeNodesInOrder(nodePage.Right))
							{
								yield return ofs;
							}
						}
						break;
				}
			}
		}

		/// <summary>
		/// Finds all keys greater or equal than an specific key
		/// </summary>
		/// <param name="key">key</param>
		/// <returns></returns>
		internal IEnumerable<int> FindGreaterOrEqualThanKey(T key)
		{
			foreach (var ofs in FindGreaterOrEqualThanKey(Root, key))
			{
				yield return ofs;
			}
		}

		internal IEnumerable<int> FindGreaterOrEqualThanKey(PageIndexNodeBase<T> root, T key)
		{
			while (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						var itemsPage = root as PageIndexItems<T>;
						foreach (var ofs in Index.IndexItems<T>()
							.FindByOper(itemsPage.Offset, key, TokenType.GreaterOrEqual))
						{
							yield return ofs;
						}
						//stop here
						root = null;
						break;
					case MetaIndexType.Node:
						var nodePage = root as PageIndexNode<T>;
						//compare key to node:key
						var comp = key.CompareTo(nodePage.Key);
						if (comp > 0)
						{
							//continue probing right node
							root = nodePage.Right;
						}
						else
						{
							if (comp < 0)
							{
								//probe left node
								foreach (var ofs in FindGreaterThanKey(nodePage.Left, key))
								{
									yield return ofs;
								}
							}

							//return root values
							foreach (var ofs in nodePage.Values)
							{
								yield return ofs;
							}

							//return all right nodes -in-order
							foreach (var ofs in DumpTreeNodesInOrder(nodePage.Right))
							{
								yield return ofs;
							}
						}
						break;
				}
			}
		}

		internal IEnumerable<int> DumpTreeNodesInOrder(PageIndexNodeBase<T> root)
		{
			var stack = new Stack<PageIndexNodeBase<T>>();
			PageIndexNode<T> nodePage = null;

			//set current to root
			PageIndexNodeBase<T> current = root;

			IEnumerable<int> EnumerateOffsets(PageIndexItems<T> page)
			{
				//find from dictionary for fast search
				var metaPage = Index.IndexItems<T>()[page.Offset];
				if (metaPage != null)
				{
					foreach (var ofs in metaPage.Items.SelectMany(i => i.Value))
					{
						yield return ofs;
					}
				}
				else
				{
					yield break;
				}
			}

			while (stack.Count > 0 || current != null)
			{
				if (current != null)
				{
					//if it's a items page, return all it's values
					if (current.Type == MetaIndexType.Items)
					{
						//find items page by its offset
						foreach (var ofs in EnumerateOffsets(current as PageIndexItems<T>))
						{
							yield return ofs;
						}
						//signal end
						current = null;
					}
					else
					{
						//it's a node page, push current page
						stack.Push(current);

						//try to go Left
						current = ((PageIndexNode<T>)current).Left;
					}
				}
				else
				{
					current = stack.Pop();
					//return current node page items --first--

					if (current.Type == MetaIndexType.Items)
					{
						//find items page by its offset
						foreach (var ofs in EnumerateOffsets(current as PageIndexItems<T>))
						{
							yield return ofs;
						}
						//signal end
						current = null;
					}
					else
					{
						//it's a node page, return Key values, and try to go Right
						nodePage = current as PageIndexNode<T>;
						foreach (var ofs in nodePage.Values)
						{
							yield return ofs;
						}
						current = nodePage.Right;
					}
				}
			}
		}

		/// <summary>
		/// Finds a key in the key tree of nodes
		/// </summary>
		/// <param name="key">key</param>
		/// <returns></returns>
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
						//compare key to node:key
						var comp = key.CompareTo(nodePage.Key);
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

		internal PageIndexNode(int flags, int number, io.BinaryReader reader, DbColumnType keyType)
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
