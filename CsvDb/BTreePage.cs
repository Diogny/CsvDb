using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;
using System.Linq;

namespace CsvDb
{
	public abstract class BTreePageBase<T>
		where T : IComparable<T>
	{
		public BTreePageBase<T> Parent { get; set; }

		public bool IsRoot { get { return Parent == null; } }

		public abstract bool IsLeaf { get; }

		public abstract bool HasRoot { get; }

		public bool Sealed { get; protected internal set; }

		public int Index { get; set; }

		public abstract BTreePageTypeEnum Type { get; }

		public abstract byte[] ToBuffer();

		public abstract int ChildrenCount { get; }

		public BTreePageBase(bool isSealed)
		{
			Sealed = isSealed;
		}

	}

	public class BTreePageNode<T> : BTreePageBase<T>
		where T : IComparable<T>
	{
		public BTreePageBase<T> Left { get; set; }

		public BTreePageBase<T> Right { get; set; }

		public override bool IsLeaf => Left == null && Right == null;

		public override BTreePageTypeEnum Type => BTreePageTypeEnum.Node;

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

		public override bool HasRoot { get { return true; } }

		public BTreePageNode(KeyValuePair<T, List<int>> root)
			: base(true)
		{
			Root = root;
			Sealed = true;
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

	public class BTreePageItems<T> : BTreePageBase<T>
		where T : IComparable<T>
	{
		public override bool HasRoot { get { return true; } }

		public override bool IsLeaf => true;

		public override int ChildrenCount => 1;

		public override BTreePageTypeEnum Type => BTreePageTypeEnum.Collection;


		/// <summary>
		/// Offset 0-based of the start of this page
		/// </summary>
		public int Offset { get; set; }

		public List<KeyValuePair<T, List<int>>> Items { get; set; }

		public BTreePageItems(IEnumerable<KeyValuePair<T, List<int>>> items = null)
			: base(false)
		{
			Items = new List<KeyValuePair<T, List<int>>>();
			if (items != null)
			{
				Items.AddRange(items);
			}
		}

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
