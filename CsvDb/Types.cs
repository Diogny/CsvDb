using System;
using System.Collections.Generic;
using System.Dynamic;
using io = System.IO;

namespace CsvDb
{
	//it's saved as an Int32
	//keep in same order in TokenType
	public enum DbColumnType : UInt32
	{
		None = 0b000000000,
		Byte = 0b000000010,  // 2
		Int16 = 0b00000100,  // 4
		Int32 = 0b00001000,  // 8
		String = 0b00010000, // 16
		Double = 0b00100000, // 32
		Char = 0b01000000,   // 64
		Single = 0b10000000,  // 128
		Decimal = 0b100000000,  // 256
		Int64 = 0b1000000000,  // 512
		Bool = 0b10000000000,  // 1024
	}

	/// <summary>
	/// database schema configuration type
	/// </summary>
	public enum DbSchemaConfigType : Int32
	{
		None = 0b00000000,
		/// <summary>
		/// text/csv schema (1)
		/// </summary>
		Csv = 0b000000001,
		/// <summary>
		/// binary schema (2)
		/// </summary>
		Binary = 0b000000010
	}

	/// <summary>
	/// binary indexer page tree type
	/// </summary>
	public enum BTreePageType
	{
		/// <summary>
		/// a binary tree page structure
		/// </summary>
		Node,
		/// <summary>
		/// a leaf collection of items
		/// </summary>
		Collection
	}

	/// <summary>
	/// expression operand type
	/// </summary>
	public enum OperandType
	{
		/// <summary>
		/// error or undefined
		/// </summary>
		None,
		/// <summary>
		/// char/string operand
		/// </summary>
		String,
		/// <summary>
		/// any number operand
		/// </summary>
		Number,
		/// <summary>
		/// database table column operand
		/// </summary>
		Column
	}

	/// <summary>
	/// token types
	/// </summary>
	public enum TokenType : int
	{
		None = 0,
		SELECT,
		FROM,
		WHERE,
		TOP,
		LIMIT,
		ROWNUM,
		AND,
		OR,
		NOT,
		JOIN,
		INNER,
		LEFT,
		RIGHT,
		OUTER,
		FULL,
		CROSS,
		ON,
		IS,
		NULL,
		EXISTS,
		COUNT,
		AVG,
		SUM,
		MIN,
		MAX,
		AS,
		DISTINCT,
		BETWEEN,
		LIKE,
		IN,
		ORDER,
		BY,
		ASC,
		DESC,
		INSERT,
		INTO,
		VALUES,
		UPDATE,
		SET,
		DELETE,
		PERCENT,
		CONCAT,
		//keep in same order in DbColumnType
		Byte,
		Int16,
		Int32,
		String,
		Double,
		Char,
		Float,
		Decimal,
		Int64,
		/// <summary>
		/// variable
		/// </summary>
		Identifier,
		/// <summary>
		/// any number
		/// </summary>
		Number,
		Column,
		Assign,
		//comparison operators  = <> > >= < <= 
		//do not change or update Utils.IsComparison
		Equal,
		NotEqual,
		Greater,
		GreaterOrEqual,
		Less,
		LessOrEqual,
		// (
		OpenPar,
		// )
		ClosePar,
		// .
		Dot,
		// -
		Minus,
		// +
		Plus,
		// *
		Astherisk,
		// ,
		Comma,
		// ;  it's the end of an SQL query
		SemiColon
	}

	/// <summary>
	/// static class of constants
	/// </summary>
	public static class Consts
	{
		//
		public const Int32 BTreePageNodeFlag = 0b00000001;

		public const Int32 BTreePageItemsFlag = 0b00000010;

		public const Int32 BTreePageNodeItemsFlag = 0b00000011;

		//
		public const Int32 BTreeUniqueKeyValueFlag = 0b00000100;

		public const Int32 IndexHeaderIsUnique = 0b00100000000;

		public const Int32 IndexHeaderIsKey = 0b01000000000;

		public const Int32 IndexHeaderIsLeaf = 0b10000000000;

		/// <summary>
		/// This sets the last keyword token. If TokenType changes, change THIS TOO!!!!!
		/// </summary>
		public const DbColumnType LastKeywordToken = DbColumnType.Int64;
	}

	/// <summary>
	/// database generator struct
	/// </summary>
	struct IndexStruct
	{
		public DbColumn column;

		public string file;

		public io.StreamWriter writer;
	}

	/// <summary>
	/// database generator class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class DbKeyValues<T>
	{
		public T Key { get; set; }

		public List<int> Values { get; set; }
	}

	/// <summary>
	/// testings
	/// </summary>
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
