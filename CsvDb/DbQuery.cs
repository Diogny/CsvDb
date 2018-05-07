using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;

namespace CsvDb
{
	/// <summary>
	/// SQL Query class
	/// </summary>
	public partial class DbQuery
	{

		/// <summary>
		/// SELECT column0, column1,...
		/// </summary>
		public ColumnsSelect Select { get; }

		/// <summary>
		/// FROM [table name] AS [alias], [table name] AS [alias], ...
		/// </summary>
		public List<Table> From { get; }

		/// <summary>
		/// SQL Join
		/// </summary>
		public SqlJoin Join { get; }

		/// <summary>
		/// WHERE expr0 AND|OR exper1 ...
		/// </summary>
		public ColumnsWhere Where { get; }

		/// <summary>
		/// Enumerate all tables used in the query
		/// </summary>
		public IEnumerable<Table> Tables
		{
			get
			{
				IEnumerable<Table> collection = From;
				if (Join != null)
				{
					collection = collection.Append(Join.Table);
				}
				foreach (var table in collection)
				{
					yield return table;
				}
			}
		}

		/// <summary>
		/// Creates an SQL query
		/// </summary>
		/// <param name="select">column SELECT</param>
		/// <param name="from">FROM tables</param>
		/// <param name="join">JOIN statement</param>
		/// <param name="where">WHERE expressions</param>
		/// <param name="limit">query LIMIT</param>
		internal DbQuery(
			ColumnsSelect select,
			IEnumerable<Table> from,
			SqlJoin join,
			ColumnsWhere where)
		{
			if ((Select = select) == null)
			{
				throw new ArgumentException($"SELECT columns cannot be empty or null");
			}
			if (from == null || (From = new List<Table>(from)).Count == 0)
			{
				throw new ArgumentException($"FROM tables cannot be empty or null");
			}
			Where = where;
			Join = join;
		}

		public override string ToString()
		{
			var items = new List<string>
			{
				"SELECT",
				Select.ToString(),
				"FROM",
				String.Join(", ", From)
			};

			if (Join != null)
			{
				items.Add(Join.ToString());
			}

			items.Add(Where.ToString());

			return String.Join(' ', items);
		}

		/// <summary>
		/// alias contract
		/// </summary>
		public interface IAlias
		{
			/// <summary>
			/// Gets the AS alias
			/// </summary>
			String Alias { get; }

			/// <summary>
			/// True if it has an alias
			/// </summary>
			bool HasAlias { get; }
		}

		/// <summary>
		/// name contract
		/// </summary>
		public interface IName
		{
			/// <summary>
			/// name
			/// </summary>
			String Name { get; }
		}

		/// <summary>
		/// Represents a named alias object
		/// </summary>
		public abstract class NamedAlias : IAlias, IName
		{
			/// <summary>
			/// AS alias
			/// </summary>
			public string Alias { get; }

			/// <summary>
			/// true if has an alias
			/// </summary>
			public bool HasAlias => Alias != null;

			/// <summary>
			/// name
			/// </summary>
			public string Name { get; }

			/// <summary>
			/// Creates a named alias object
			/// </summary>
			/// <param name="name">name</param>
			/// <param name="alias">AS alias</param>
			public NamedAlias(string name, string alias)
			{
				if (String.IsNullOrWhiteSpace(name))
				{
					throw new ArgumentException("Named alias identifier must have a name");
				}
				Name = name.Trim();
				Alias = String.IsNullOrWhiteSpace(alias) ? null : alias.Trim();
			}

			public override string ToString() => $"{Name}{(HasAlias ? $" AS {Alias}" : String.Empty)}";
		}

		/// <summary>
		/// Represents an SQL Query table with/without an alias
		/// </summary>
		public class Table : NamedAlias
		{

			/// <summary>
			/// Creates an SQL Query table with/without an alias
			/// </summary>
			/// <param name="name">table name</param>
			/// <param name="alias">[table name] AS [alias]</param>
			public Table(string name, string alias = null)
				: base(name, alias)
			{ }

		}

		/// <summary>
		/// Represents an SQL Query column with/without table alias, an alias
		/// </summary>
		public class Column : NamedAlias
		{
			/// <summary>
			/// [TableAlias].[column name]
			/// </summary>
			public string TableAlias { get; }

			public bool HasTableAlias => TableAlias != null;

			IColumnMeta _meta;
			/// <summary>
			/// Gets the column index in the table
			/// </summary>
			public IColumnMeta Meta
			{
				get { return _meta; }
				internal set
				{
					if ((_meta = value) != null)
					{
						//set new hash value
						Hash = $"{_meta.TableName}.{Name}";
					} else
					{
						//this will be an error if executed
						Hash = null;
					}
				}
			}

			public string Hash { get; private set; }

			/// <summary>
			/// Creates an SQL Query column with/without an alias
			/// </summary>
			/// <param name="name">column name</param>
			/// <param name="tableAlias">[table alias].[column name]</param>
			/// <param name="alias">">[table alias].[column name] AS [alias]</param>
			public Column(string name, string tableAlias = null, string alias = null)
				: base(name, alias)
			{
				TableAlias = String.IsNullOrWhiteSpace(tableAlias) ? null : tableAlias.Trim();
				Meta = null;
			}

			public Column(string name, IColumnMeta meta)
				: base(name, null)
			{
				Meta = meta;
			}

			public string Identifier() => $"{(HasTableAlias ? $"{TableAlias}." : String.Empty)}{Name}";

			public override string ToString() => $"{(HasTableAlias ? $"{TableAlias}." : String.Empty)}{base.ToString()}";
		}

		/// <summary>
		/// Parsed Sql query single token
		/// </summary>
		public class TokenItem
		{
			/// <summary>
			/// token
			/// </summary>
			public TokenType Token { get; }

			/// <summary>
			/// value or text of the token
			/// </summary>
			public String Value { get; }

			/// <summary>
			/// starting position of the token
			/// </summary>
			public int Position { get; }

			/// <summary>
			/// length of the value or text of the token
			/// </summary>
			public int Length => Value.Length;

			/// <summary>
			/// creates a token item
			/// </summary>
			/// <param name="token">token</param>
			/// <param name="position">start position</param>
			internal TokenItem(TokenType token, int position)
				: this(token, null, position)
			{ }

			/// <summary>
			/// creates a token item
			/// </summary>
			/// <param name="token">token</param>
			/// <param name="value">text</param>
			/// <param name="position">start position</param>
			internal TokenItem(TokenType token, string value, int position)
			{
				Token = token;
				Value = value;
				Position = position;
			}

			public override string ToString() => $"({Token}) {Value} @{Position}";

			public static TokenItem Empty() { return new TokenItem(TokenType.None, null, -1); }
		}

		public class ColumnsWhere
		{
			public ExpressionItem Root { get; }

			/// <summary>
			/// returns true if we got a WHERE clause in the SQL query
			/// </summary>
			public bool Defined => Root != null;

			public IEnumerable<Column> Columns => Root != null ? Root.GetColumns() : Enumerable.Empty<Column>();

			public ColumnsWhere(ExpressionItem root = null)
			{
				Root = root;
			}

			public override string ToString()
			{
				return Root == null ? String.Empty : $"WHERE {Root.ToString()}";
			}

		}

	}
}
