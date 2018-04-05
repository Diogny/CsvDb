using CsvDb.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CsvDb
{
	internal class DbQueryExecuter<T>
		where T : IComparable<T>
	{
		public bool IsExpression => Expression != null;

		public DbQueryExpression Expression { get; protected internal set; }

		public DbQueryTableColumnOperand Table { get; protected internal set; }

		public DbColumn Column { get; protected internal set; }

		public DbQueryConstantOperand Constant { get; protected internal set; }

		public DbQueryExecuter(DbColumn column)
		{
			Column = column;
		}

		public DbQueryExecuter(DbQueryExpression expression)
		{
			Expression = expression;

			//Action<CsvDbQueryOperand, CsvDbQueryOperand> Assign = (table, constant) =>
			void Assign(DbQueryOperand table, DbQueryOperand constant)
			{
				Table = table as DbQueryTableColumnOperand;
				Column = Table.Column;
				Constant = constant as DbQueryConstantOperand;
			}

			//get table and constant values
			if (expression.Left.IsTableColumn)
			{
				Assign(expression.Left, expression.Right);
			}
			else if (expression.Right.IsTableColumn)
			{
				Assign(expression.Right, expression.Left);
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
				var nodeTree = Table.Column.IndexTree<T>();
				int offset = -1;

				var value = Constant.Value();
				Type valueType = Type.GetType($"System.{Constant.OperandType}");

				var key = (T)Convert.ChangeType(value, valueType);

				var collection = Enumerable.Empty<int>();

				if (nodeTree.Root == null)
				{
					//go to .bin file directly, it's ONE page of items
					collection = Table.Column.IndexItems<T>()
						.FindByOper(DbGenerator.ItemsPageStart, key, Expression.Operator.OperatorType);
				}
				else
				{
					//this's for tree node page structure
					switch (Expression.Operator.Name)
					{
						case "=":
							var baseNode = nodeTree.FindKey(key) as PageIndexNodeBase<T>;
							if (baseNode == null)
							{
								throw new ArgumentException($"Index corrupted");
							}

							switch (baseNode.Type)
							{
								case MetaIndexType.Node:
									var nodePage = baseNode as PageIndexNode<T>;
									if (nodePage != null && nodePage.Values != null)
									{
										collection = nodePage.Values;
									}
									break;
								case MetaIndexType.Items:
									offset = ((PageIndexItems<T>)baseNode).Offset;
									DbKeyValues<T> item = Table.Column.IndexItems<T>().Find(offset, key);
									if (item != null && item.Values != null)
									{
										collection = item.Values;
									}
									break;
							}
							break;
						case "<":
							collection = nodeTree.FindLessThanKey(key);
							break;
						case "<=":
							collection = nodeTree.FindLessOrEqualThanKey(key);
							break;
						case ">":
							collection = nodeTree.FindGreaterThanKey(key);
							break;
						case ">=":
							collection = nodeTree.FindGreaterOrEqualThanKey(key);
							break;
							//throw new ArgumentException($"Operator: {Expression.Operator.Name} not implemented yet!");
					}
				}
				//
				foreach (var ofs in collection)
				{
					yield return ofs;
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
					//In-Order return all values
					foreach (var offs in treeReader.DumpTreeNodesInOrder(treeReader.Root)) //DumpFullTable(treeReader.Root))
					{
						yield return offs;
					}
				}
			}

			yield break;
		}

		//IEnumerable<int> DumpFullTable(PageIndexNodeBase<T> root)
		//{
		//	var stack = new Stack<PageIndexNodeBase<T>>();
		//	PageIndexNode<T> nodePage = null;

		//	//set current to root
		//	PageIndexNodeBase<T> current = root;

		//	IEnumerable<int> EnumerateOffsets(PageIndexItems<T> page)
		//	{
		//		//find from dictionary for fast search
		//		var metaPage = Column.IndexItems<T>()[page.Offset];
		//		if (metaPage != null)
		//		{
		//			foreach (var ofs in metaPage.Items.SelectMany(i => i.Value))
		//			{
		//				yield return ofs;
		//			}
		//		}
		//		else
		//		{
		//			yield break;
		//		}
		//	}

		//	while (stack.Count > 0 || current != null)
		//	{
		//		if (current != null)
		//		{
		//			//if it's a items page, return all it's values
		//			if (current.Type == MetaIndexType.Items)
		//			{
		//				//find items page by its offset
		//				foreach (var ofs in EnumerateOffsets(current as PageIndexItems<T>))
		//				{
		//					yield return ofs;
		//				}
		//				//signal end
		//				current = null;
		//			}
		//			else
		//			{
		//				//it's a node page, push current page
		//				stack.Push(current);

		//				//try to go Left
		//				current = ((PageIndexNode<T>)current).Left;
		//			}
		//		}
		//		else
		//		{
		//			current = stack.Pop();
		//			//return current node page items --first--

		//			if (current.Type == MetaIndexType.Items)
		//			{
		//				//find items page by its offset
		//				foreach (var ofs in EnumerateOffsets(current as PageIndexItems<T>))
		//				{
		//					yield return ofs;
		//				}
		//				//signal end
		//				current = null;
		//			}
		//			else
		//			{
		//				//it's a node page, return Key values, and try to go Right
		//				nodePage = current as PageIndexNode<T>;
		//				foreach (var ofs in nodePage.Values)
		//				{
		//					yield return ofs;
		//				}
		//				current = nodePage.Right;
		//			}
		//		}
		//	}
		//}
	}
}
