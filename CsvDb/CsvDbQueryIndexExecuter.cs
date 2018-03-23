using System;
using System.Collections.Generic;
using System.Linq;
using static CsvDb.CsvDbQuery;

namespace CsvDb
{
	internal class CsvDbQueryIndexExecuter<T>
		where T : IComparable<T>
	{
		public bool IsExpression => Expression != null;

		public CsvDbQueryExpression Expression { get; protected internal set; }

		public CsvDbQueryTableColumnOperand Table { get; protected internal set; }

		public CsvDbColumn Column { get; protected internal set; }

		public CsvDbQueryConstantOperand Constant { get; protected internal set; }

		public CsvDbQueryIndexExecuter(CsvDbColumn column)
		{
			Column = column;
		}

		public CsvDbQueryIndexExecuter(CsvDbQueryExpression expression)
		{
			Expression = expression;

			//Action<CsvDbQueryOperand, CsvDbQueryOperand> Assign = (table, constant) =>
			void Assign(CsvDbQueryOperand table, CsvDbQueryOperand constant)
			{
				Table = table as CsvDbQueryTableColumnOperand;
				Column = Table.Column;
				//
				Constant = constant as CsvDbQueryConstantOperand;
			}

			//get table and constant values
			if (expression.Left.IsTableColumn)
			{
				Assign(expression.Left, expression.Right);
				//Table = expression.Left as CsvDbQueryTableColumnOperand;
				//Constant = expression.Right as CsvDbQueryConstantOperand;
			}
			else if (expression.Right.IsTableColumn)
			{
				Assign(expression.Right, expression.Left);
				//Table = expression.Right as CsvDbQueryTableColumnOperand;
				//Constant = expression.Left as CsvDbQueryConstantOperand;
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
			if (IsExpression)
			{
				switch (Expression.Operator.Name)
				{
					case "=":
						//check if IEnumerable is called twice when reading tree node pages



						var treeReader = Table.Column.IndexTree<T>();
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
							var baseNode = treeReader.FindKey(key) as PageIndexNodeBase<T>;
							if (baseNode == null)
							{
								throw new ArgumentException($"Index corrupted");
							}
							switch (baseNode.Type)
							{
								case MetaIndexType.Node:
									var nodePage = baseNode as PageIndexNode<T>;
									foreach (var csvOfs in nodePage.Values)
									{
										yield return csvOfs;
									}
									break;
								case MetaIndexType.Items:
									offset = ((PageIndexItems<T>)baseNode).Offset;
									CsvDbKeyValues<T> item = Table.Column.IndexItems<T>().Find(offset, key);
									foreach (var cvsOfs in item.Values)
									{
										yield return cvsOfs;
									}
									break;
							}
						}
						break;
					case ">":
					case ">=":
					//return FindGreaterThan(column, oper, key);
					case "<":
					case "<=":
						//return FindLessThan(column, oper, key);
						throw new ArgumentException($"Operator: {Expression.Operator.Name} not implemented yet!");
				}
			}
			else
			{
				//return the full table rows
				var treeReader = Column.IndexTree<T>();
				if (treeReader.Root == null)
				{
					//itemspage has only one page, no tree root
					foreach (var cvsOfs in Column.IndexItems<T>()
						.Pages[0].Items.SelectMany(i => i.Value))
					{
						yield return cvsOfs;
					}
				}
				else
				{
					foreach (var offs in DumpTable(treeReader.Root))
					{
						yield return offs;
					}
				}
			}

			yield break;
		}

		IEnumerable<int> DumpTable(PageIndexNodeBase<T> root)
		{
			if (root != null)
			{
				switch (root.Type)
				{
					case MetaIndexType.Items:
						var itemPage = root as PageIndexItems<T>;
						yield return itemPage.Offset;
						break;
					case MetaIndexType.Node:
						var nodePage = root as PageIndexNode<T>;
						//return first all left nodes
						foreach (var ofs in DumpTable(nodePage.Left))
							yield return ofs;
						//return root node
						foreach (var ofs in nodePage.Values)
							yield return ofs;
						//return last all right nodes
						foreach (var ofs in DumpTable(nodePage.Right))
							yield return ofs;
						break;
				}
			}
		}

	}
}
