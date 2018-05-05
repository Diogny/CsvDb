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
		public ColumnsSelect Columns { get; }

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
		/// -1 is not selected
		/// </summary>
		public int Limit { get; protected internal set; }

		/// <summary>
		/// Creates an SQL query
		/// </summary>
		/// <param name="columns">column SELECT</param>
		/// <param name="from">FROM tables</param>
		/// <param name="join">JOIN statement</param>
		/// <param name="where">WHERE expressions</param>
		/// <param name="limit">query LIMIT</param>
		internal DbQuery(
			ColumnsSelect columns,
			IEnumerable<Table> from,
			SqlJoin join,
			ColumnsWhere where,
			int limit = -1)
		{
			if ((Columns = columns) == null)
			{
				throw new ArgumentException($"SELECT columns cannot be empty or null");
			}
			if (from == null || (From = new List<Table>(from)).Count == 0)
			{
				throw new ArgumentException($"FROM tables cannot be empty or null");
			}

			Where = where;

			Join = join;
			Limit = limit <= 0 ? -1 : limit;
		}

		public override string ToString()
		{
			var items = new List<string>
			{
				"SELECT",
				Columns.ToString(),
				"FROM",
				String.Join(", ", From)
			};

			if (Join != null)
			{
				items.Add(Join.ToString());
			}

			items.Add(Where.ToString());

			if (Limit > 0)
			{
				items.Add($"LIMIT {Limit}");
			}

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

			/// <summary>
			/// Gets the column index in the table
			/// </summary>
			public IColumnMeta Meta { get; internal set; }

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
			/// Number, String, Identifier
			/// </summary>
			public bool IsIdentifier
			{
				get
				{
					return Token == TokenType.Number || Token == TokenType.String || Token == TokenType.Identifier;
				}
			}

			/// <summary>
			/// operator
			/// </summary>
			public bool IsOperator
			{
				get
				{
					return Token == TokenType.Equal || Token == TokenType.NotEqual ||
						Token == TokenType.Less || Token == TokenType.LessOrEqual ||
						Token == TokenType.Greater || Token == TokenType.GreaterOrEqual;
				}
			}

			/// <summary>
			/// logical AND, OR
			/// </summary>
			public bool IsLogical
			{
				get
				{
					return Token == TokenType.AND || Token == TokenType.OR;
				}
			}

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
				if ((Position = position) < 0)
				{
					throw new ArgumentException("Invalid token item");
				}
			}

			public override string ToString() => $"({Token}) {Value} @{Position}";

		}

		public class ColumnsWhere
		{
			//it's a tree, fix later
			public List<ExpressionBase> Where { get; }

			public IEnumerable<Column> Columns
			{
				get
				{
					foreach (var item in Where)
					{
						if (item.Type == ExpressionEnum.Expression)
						{
							var expr = item as Expression;
							if (expr.Left.IsColumn)
							{
								yield return (expr.Left as ColumnOperand).Column;
							}
							if (expr.Right.IsColumn)
							{
								yield return (expr.Right as ColumnOperand).Column;
							}
						}
					}
				}
			}

			public ColumnsWhere(IEnumerable<ExpressionBase> where)
			{
				//ensure never null
				Where = (where == null) ? new List<ExpressionBase>() : new List<ExpressionBase>(where);
			}

			public override string ToString()
			{
				if (Where.Count > 0)
				{
					return $"WHERE {String.Join(' ', Where)}";
				}
				return string.Empty;
			}

		}
	}
}
