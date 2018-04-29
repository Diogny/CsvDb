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
		public List<ExpressionBase> Where { get; }

		/// <summary>
		/// Enumerate all tables used in the query
		/// </summary>
		public IEnumerable<Table> Tables
		{
			get
			{
				foreach (var table in From.Append(Join.Table))
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
			IEnumerable<ExpressionBase> where,
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
			//ensure never null
			Where = (where == null) ? new List<ExpressionBase>() : new List<ExpressionBase>(where);

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

			if (Where.Count > 0)
			{
				items.Add("WHERE");
				items.Add(String.Join(' ', Where));
			}

			if (Limit > 0)
			{
				items.Add($"LIMIT {Limit}");
			}

			return String.Join(' ', items);
		}

		public interface IAlias
		{
			/// <summary>
			/// Gets the alias
			/// </summary>
			String Alias { get; }

			/// <summary>
			/// True if it has an alias
			/// </summary>
			bool HasAlias { get; }
		}

		public interface IName
		{
			String Name { get; }
		}

		public abstract class NamedAlias : IAlias, IName
		{
			public string Alias { get; }

			public bool HasAlias => Alias != null;

			public string Name { get; }

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

			public bool HasTableAlias { get { return !String.IsNullOrWhiteSpace(TableAlias); } }

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

	}
}
