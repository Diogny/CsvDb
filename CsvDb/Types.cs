using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	public enum DbColumnTypeEnum : byte
	{
		None = 0b000000000,
		Byte = 0b000000010,  // 2
		Int16 = 0b00000100,  // 4
		Int32 = 0b00001000,  // 8
		String = 0b00010000, // 16
		Double = 0b00100000, // 32
	}

	public enum BTreePageTypeEnum
	{
		Node,
		Collection
	}

	public static class Consts
	{
		public const Int32 BTreePageNodeFlag = 0b00000001;

		public const Int32 BTreePageItemsFlag = 0b00000010;

		public const Int32 BTreePageNodeItemsFlag = 0b00000011;

		//

		public const Int32 BTreeUniqueKeyValueFlag = 0b00000100;

		public const Int32 IndexHeaderIsUnique = 0b00100000000;

		public const Int32 IndexHeaderIsKey = 0b01000000000;

		public const Int32 IndexHeaderIsLeaf = 0b10000000000;

	}

	struct IndexStruct
	{
		public DbColumn column;

		public string file;

		public io.StreamWriter writer;
	}

	public class DbKeyValues<T>
	{
		public T Key { get; set; }

		public List<int> Values { get; set; }
	}

	public class DynamicEntity : DynamicObject
	{
		private IDictionary<string, object> _values;

		public object this[string prop]
		{
			get
			{
				return _values.TryGetValue(prop, out object value) ? value : null;
			}
		}

		public T Get<T>(string prop)
			where T : IComparable<T>
		{
			return (T)this[prop];
		}

		public DynamicEntity(IDictionary<string, object> values)
		{
			_values = values;
		}

		public override bool TryGetMember(GetMemberBinder binder, out object result)
		{
			if (_values.ContainsKey(binder.Name))
			{
				result = _values[binder.Name];
				return true;
			}
			result = null;
			return false;
		}
	}
	/*
var values = new Dictionary<string, object>();
values.Add("Title", "Hello World!");
values.Add("Text", "My first post");
values.Add("Tags", new[] { "hello", "world" });

var post = new DynamicEntity(values);

dynamic dynPost = post;
var text = dynPost.Text;
	 */
}
