using System;
using System.Collections.Generic;
using System.Dynamic;
using io = System.IO;

namespace CsvDb
{
	//it's saved as an Int32
	//keep in same order in TokenType
	public enum DbColumnTypeEnum : UInt32
	{
		None = 0b000000000,
		Byte = 0b000000010,  // 2
		Int16 = 0b00000100,  // 4
		Int32 = 0b00001000,  // 8
		String = 0b00010000, // 16
		Double = 0b00100000, // 32
		Char = 0b01000000,   // 64
		Float = 0b10000000,  // 128
		Decimal = 0b100000000,  // 256
		Int64 = 0b1000000000,  // 512
													 //Single = 0b10000000000,  // 1024
	}

	//
	//public enum OperandCastEnum
	//{
	//	None,
	//	Char,
	//	String,
	//	Byte,
	//	Int16,
	//	Int32,
	//	Int64,
	//	Float,
	//	Double,
	//	Decimal
	//}

	public enum DbSchemaConfigEnum : Int32
	{
		None = 0b00000000,
		Csv = 0b000000001,  // 1
		Binary = 0b000000010,  // 2
	}

	public enum BTreePageTypeEnum
	{
		Node,
		Collection
	}

	public enum OperandEnum
	{
		None,
		String,
		Number,
		Column
	}

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
		//keep in same order in OperandCastEnum, DbColumnTypeEnum
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
		/// [table column]  agency_id
		/// </summary>
		//Column,
		/// <summary>
		/// [table name] [table descriptor]   agency a
		/// </summary>
		//TableDescriptor,
		/// <summary>
		/// [table descriptor].[column name]    a.agency_id
		/// </summary>
		//ColumnIdentifier,
		/// <summary>
		/// "Byte", "Int16", "Int32", "String", "Double"
		/// </summary>
		//Cast,
		//
		Number,
		//

		// = <> > >= < <= 
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
