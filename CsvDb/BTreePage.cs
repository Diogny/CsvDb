using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// DbGenerator Base Page generic class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public abstract class BTreePageBase<T>
		where T : IComparable<T>
	{
		/// <summary>
		/// gets the parent of the this page
		/// </summary>
		public BTreePageBase<T> Parent { get; set; }

		/// <summary>
		/// returns true if the page is the main root
		/// </summary>
		public bool IsRoot { get { return Parent == null; } }

		/// <summary>
		/// returns true if this page is an ending leaf
		/// </summary>
		public abstract bool IsLeaf { get; }

		/// <summary>
		/// returns true if this page is sealed
		/// </summary>
		public bool Sealed { get; }

		/// <summary>
		/// returns then index of this page
		/// </summary>
		public int Index { get; set; }

		/// <summary>
		/// gets the page type
		/// </summary>
		public abstract BTreePageType Type { get; }

		/// <summary>
		/// returns a buffered representation of the page
		/// </summary>
		/// <returns></returns>
		public abstract byte[] ToBuffer();

		/// <summary>
		/// gets the children count
		/// </summary>
		public abstract int ChildrenCount { get; }

		/// <summary>
		/// creates a base page class
		/// </summary>
		/// <param name="isSealed">true if sealed</param>
		public BTreePageBase(bool isSealed)
		{
			Sealed = isSealed;
		}

	}

	/// <summary>
	/// DbGenerator Tree Page Node generic class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class BTreePageNode<T> : BTreePageBase<T>
		where T : IComparable<T>
	{
		/// <summary>
		/// gets the left page node
		/// </summary>
		public BTreePageBase<T> Left { get; set; }

		/// <summary>
		/// gets the right page node
		/// </summary>
		public BTreePageBase<T> Right { get; set; }

		/// <summary>
		/// returns true if this tree page node is an ending leaf
		/// </summary>
		public override bool IsLeaf => Left == null && Right == null;

		/// <summary>
		/// tree page node
		/// </summary>
		public override BTreePageType Type => BTreePageType.Node;

		/// <summary>
		/// gets the children count of this page
		/// </summary>
		public override int ChildrenCount
		{
			get
			{
				var count = 1;
				if (Left != null)
				{
					count += Left.ChildrenCount;
				}
				if (Right != null)
				{
					count += Right.ChildrenCount;
				}
				return count;
			}
		}

		public KeyValuePair<T, List<int>> Root { get; set; }

		/// <summary>
		/// creates a tree page node
		/// </summary>
		/// <param name="root">root</param>
		public BTreePageNode(KeyValuePair<T, List<int>> root)
			: base(true)
		{
			Root = root;
		}

		/// <summary>
		/// Stores the .index tree node page structure
		/// </summary>
		/// <returns></returns>
		public override byte[] ToBuffer()
		{
			var stream = new io.MemoryStream();
			var writer = new io.BinaryWriter(stream);

			//flags
			//This's a Page Node
			Int32 valueInt32 = Consts.BTreePageNodeFlag;

			var uniqueKeyValue = Root.Value.Count == 1;
			var uniqueFlag = uniqueKeyValue ? Consts.BTreeUniqueKeyValueFlag : 0;
			valueInt32 |= uniqueFlag;
			//
			writer.Write(valueInt32);

			//page size placeholder
			writer.Write(valueInt32 = 0);

			//value
			if (uniqueKeyValue)
			{
				/*////////   UNIQUE values    ////////*/

				//just first one
				writer.Write(valueInt32 = Root.Value[0]);
			}
			else
			{
				/*////////   MULTIPLE values    ////////*/

				//count
				writer.Write(valueInt32 = Root.Value.Count);
				//values
				foreach (var value in Root.Value)
				{
					writer.Write(value);
				}
			}

			//key
			//  for multiple values, it's a unique key with multiple values
			Utils.StoreKey<T>(Root.Key, writer);

			//go back
			stream.Position = 4;
			//overwrite length
			writer.Write(valueInt32 = (Int32)stream.Length);

			return stream.ToArray();
		}

	}

	/// <summary>
	/// DbGenerator Tree Page Items generic class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class BTreePageItems<T> : BTreePageBase<T>
		where T : IComparable<T>
	{
		/// <summary>
		/// always a leaf => true
		/// </summary>
		public override bool IsLeaf => true;

		/// <summary>
		/// always => 1
		/// </summary>
		public override int ChildrenCount => 1;

		/// <summary>
		/// tree page items
		/// </summary>
		public override BTreePageType Type => BTreePageType.Collection;

		/// <summary>
		/// Offset 0-based of the start of this page in the index of items stream
		/// </summary>
		public int Offset { get; set; }

		/// <summary>
		/// List of key with their offset values in the main table data stream
		/// </summary>
		public List<KeyValuePair<T, List<int>>> Items { get; set; }

		/// <summary>
		/// creates a tree page collection of items
		/// </summary>
		/// <param name="items">collection of items</param>
		public BTreePageItems(IEnumerable<KeyValuePair<T, List<int>>> items = null)
			: base(false)
		{
			Items = new List<KeyValuePair<T, List<int>>>();
			if (items != null)
			{
				Items.AddRange(items);
			}
		}

		/// <summary>
		/// adds a new item to the collection of items
		/// </summary>
		/// <param name="item">new item</param>
		/// <returns></returns>
		public bool Add(KeyValuePair<T, List<int>> item)
		{
			Func<bool> fn = () =>
			{
				Items.Add(item);
				//check for maximum amount of items in page
				return true;
			};
			return Sealed ? false : fn();
		}

		/// <summary>
		/// gets a buffered representation of this tree page items collection
		/// </summary>
		/// <returns></returns>
		public override byte[] ToBuffer()
		{
			var stream = new io.MemoryStream();
			var writer = new io.BinaryWriter(stream);

			//flags
			Int32 valueInt32 = Consts.BTreePageItemsFlag;

			var uniqueKeyValue = Items.All(i => i.Value.Count == 1);
			var uniqueFlag = uniqueKeyValue ? Consts.BTreeUniqueKeyValueFlag : 0;
			valueInt32 |= uniqueFlag;
			//
			writer.Write(valueInt32);

			//page.Offset
			/////////////////  TO BE USED AS AN ID //////////////////////////
			writer.Write(valueInt32 = Offset);

			//page size placeholder
			writer.Write(valueInt32 = 0);

			//items count
			writer.Write(valueInt32 = Items.Count);

			//store keys
			//if T is string, store all byte length, and then all chars next
			Type type = typeof(T);
			if (type.Name == "String")
			{
				var keys = Items.GetAllKeyStrings<T>().ToList();
				//store key length bytes
				foreach (var k in keys)
				{
					byte length = (byte)k.Length;
					writer.Write(length);
				}
				//store key chars
				foreach (var k in keys)
				{
					var chars = k.ToCharArray();
					writer.Write(chars, 0, chars.Length);
				}
			}
			else
			{
				foreach (var item in Items)
				{
					item.Key.StoreKey<T>(writer);
				}
			}

			if (uniqueKeyValue)
			{
				//Store Unique single values
				foreach (var item in Items)
				{
					writer.Write(item.Value[0]);
				}

			}
			else
			{
				//Store Multiple values
				foreach (var item in Items)
				{
					// [Count: Int16]
					Int16 itemValueCount = (Int16)item.Value.Count;
					writer.Write(itemValueCount);
					//[Value or values]
					foreach (var value in item.Value)
					{
						writer.Write(valueInt32 = value);
					}
				}

			}

			//go back
			stream.Position = 8;
			//overwrite length
			writer.Write(valueInt32 = (Int32)stream.Length);

			return stream.ToArray();
		}

	}

}
