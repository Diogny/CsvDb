using System;
using System.Collections.Generic;
using static CsvDb.CsvDbQuery;

namespace CsvDb
{
	internal class CsvDbQueryIndexExecuter<T>
		where T : IComparable<T>
	{
		public CsvDbQueryExpression Expression { get; protected internal set; }

		public CsvDbQueryTableColumnOperand Table { get; protected internal set; }

		public CsvDbQueryConstantOperand Constant { get; protected internal set; }

		public CsvDbQueryIndexExecuter(CsvDbQueryExpression expression)
		{
			Expression = expression;

			//get table and constant values
			if (expression.Left.IsTableColumn)
			{
				Table = expression.Left as CsvDbQueryTableColumnOperand;
				Constant = expression.Right as CsvDbQueryConstantOperand;
			}
			else if (expression.Right.IsTableColumn)
			{
				Table = expression.Right as CsvDbQueryTableColumnOperand;
				Constant = expression.Left as CsvDbQueryConstantOperand;
			}
			else
			{
				throw new ArgumentException("WHERE expression has no table.column provided");
			}
		}

		//return a collection of offsets to the record
		public IEnumerable<int> Execute()
		{
			//fills Pages and return the amount of keys in this search

			switch (Expression.Operator.Name)
			{
				case "=":
					var treeReader = Table.Column.TreeIndexReader<T>();
					int offset = -1;

					var value = Constant.Value();
					Type valueType = Type.GetType($"System.{Constant.OperandType}");

					var key = (T)Convert.ChangeType(value, valueType);

					if (treeReader.Root == null)
					{
						//go to .bin file directly, it's ONE page of items
						offset = CsvDbGenerator.ItemsPageStart;
					}
					else
					{
						var meta = treeReader.FindKey(key) as MetaIndexItems<T>;
						if (meta == null)
						{
							throw new ArgumentException($"Index corrupted");
						}
						//
						offset = meta.Offset;
					}
					CsvDbKeyValues<T> item = Table.Column.PageItemReader<T>().Find(offset, key);
					foreach (var cvsOfs in item.Values)
					{
						yield return cvsOfs;
					}
					break;
				case ">":
				case ">=":
				//return FindGreaterThan(column, oper, key);
				case "<":
				case "<=":
					//return FindLessThan(column, oper, key);
					throw new ArgumentException("Operator: {Expression.Operator.Name} not implemented yet!");
			}

			yield break;
		}


	}
}
