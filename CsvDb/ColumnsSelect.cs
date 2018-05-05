using System;
using System.Collections.Generic;
using System.Linq;

namespace CsvDb
{
	public partial class DbQuery
	{

		/// <summary>
		/// SQL Query SELECT columns class
		/// </summary>
		public class ColumnsSelect
		{

			/// <summary>
			/// COUNT, AVG, SUM, MIN, MAX
			/// </summary>
			public TokenType Function { get; }

			/// <summary>
			/// COUNT (column name)
			/// </summary>
			public Column Column { get { return Columns[0]; } }

			/// <summary>
			/// COUNT (column name) AS alias
			/// </summary>
			public string FunctionAlias { get; }

			/// <summary>
			/// true if SELECT function has an alias
			/// </summary>
			public bool HasFunctionAlias { get { return !String.IsNullOrWhiteSpace(FunctionAlias); } }

			/// <summary>
			/// true if it's a SELECT function (COUNT, SUM, AVG, MIN, MAX...)
			/// </summary>
			public bool IsFunction { get { return Function != TokenType.None; } }

			/// <summary>
			/// SELECT column header
			/// </summary>
			public string[] Header
			{
				get
				{
					if (IsFunction)
					{
						return new string[] { HasFunctionAlias ? FunctionAlias : Function.ToString() };
					}
					else
					{
						return Columns.Select(c => c.HasAlias ? c.Alias : c.Name).ToArray();
					}
				}
			}

			/// <summary>
			/// SELECT TOP [PERCENT] integer column0, column1 AS alias
			/// </summary>
			public int Top { get; }

			/// <summary>
			/// SELECT column0 AS alias0, column1 AS alias1,...
			/// </summary>
			public List<Column> Columns { get; }

			/// <summary>
			/// SELECT *
			/// </summary>
			public bool FullColumns { get; }

			/// <summary>
			/// creates a SELECT function
			/// </summary>
			/// <param name="function">COUNT, SUM, AVG, MIN, MAX</param>
			/// <param name="column">function column name</param>
			/// <param name="alias">function alias</param>
			internal ColumnsSelect(TokenType function, Column column, string alias)
			{
				Function = function;
				FunctionAlias = alias;
				Columns = new List<Column>() { column };
				//
				FullColumns = false;
			}

			/// <summary>
			/// creates a SELECT * full column
			/// </summary>
			/// <param name="top">top value</param>
			internal ColumnsSelect(int top)
				: this(top, null)
			{ }

			/// <summary>
			/// creates a SELECT column0, column1,...
			/// </summary>
			/// <param name="top">top value</param>
			/// <param name="columnCollection">column collection</param>
			internal ColumnsSelect(int top, IEnumerable<Column> columnCollection)
			{
				Top = (top <= 0) ? -1 : top;

				Columns = (columnCollection == null) ?
					new List<Column>() :
					new List<Column>(columnCollection);
				FullColumns = Columns.Count == 0;
				//
				Function = TokenType.None;
				FunctionAlias = null;
			}

			/// <summary>
			/// adds a column, for SELECT * only
			/// </summary>
			/// <param name="column">SELECT column</param>
			/// <returns></returns>
			internal bool Add(Column column)
			{
				if (FullColumns && column != null)
				{
					Columns.Add(column);
					return true;
				}
				return false;
			}

			public override string ToString()
			{
				if (IsFunction)
				{
					return $"{Function}({Column}){(HasFunctionAlias ? $" AS {FunctionAlias}" : String.Empty)}";
				}
				else if (FullColumns)
				{
					return "*";
				}
				else
				{
					return $"{(Top > 0 ? $"TOP {Top} " : String.Empty)}{String.Join(", ", Columns)}";
				}
			}

		}
	}
}
