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
			public Column Column { get; }

			/// <summary>
			/// COUNT (column name) AS alias
			/// </summary>
			public string FunctionAlias { get; }

			public bool HasFunctionAlias { get { return !String.IsNullOrWhiteSpace(FunctionAlias); } }

			public bool IsFunction { get { return Function != TokenType.None; } }

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


			public IEnumerable<Column> AllColumns
			{
				get
				{
					if (IsFunction)
					{
						yield return Column;
					}
					else
					{
						foreach (var col in Columns)
						{
							yield return col;
						}
					}
				}
			}

			internal ColumnsSelect(TokenType function, Column column, string alias)
			{
				Function = function;
				Column = column;
				FunctionAlias = alias;
				//
				FullColumns = false;
				Columns = new List<Column>();
			}

			internal ColumnsSelect(int top)
				: this(top, null)
			{ }

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
