using System;
using System.Collections.Generic;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// Query column executer
	/// </summary>
	/// <typeparam name="T"></typeparam>
	internal class DbQueryExecuter<T>
		where T : IComparable<T>
	{
		public bool IsExpression => Expression != null;

		public DbQuery.Expression Expression { get; protected internal set; }

		public DbColumn Column { get; protected internal set; }

		public DbQuery.ConstantOperand Constant { get; protected internal set; }

		public DbQueryExecuter(DbColumn column)
		{
			Column = column;
		}

		public DbQueryExecuter(DbQuery.Expression expression, CsvDb db)
		{
			Expression = expression;

			//Action<CsvDbQueryOperand, CsvDbQueryOperand> Assign = (table, constant) =>
			void Assign(DbQuery.Operand column, DbQuery.Operand constant)
			{
				//Table = table as DbQuery.ColumnOperand;
				var col = column as DbQuery.ColumnOperand;
				Column = db.Index(col.Column.Meta.TableName, column.Text);
				Constant = constant as DbQuery.ConstantOperand;
			}

			//get table and constant values
			if (expression.Left.IsColumn)
			{
				Assign(expression.Left, expression.Right);
			}
			else if (expression.Right.IsColumn)
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
				var collection = Enumerable.Empty<int>();

				if (!Column.Indexed)
				{
					//slow way -column is not indexed, have to read records one by one and compare column value


					//every expression should have a collection of records already retrieved so visualization
					//  have it already to display
					throw new ArgumentException($"column {Column.Name} must be indexed to be used in query expression");
				}
				else
				{
					//fast way -column is indexed
					var nodeTree = Column.IndexTree<T>();
					int offset = -1;

					if(Column.TypeEnum != Constant.Type)
					{
						//try to convert constant value to column type

					}

					Type valueType = Type.GetType($"System.{Constant.Type}");

					var value = Constant.Value();
					var key = (T)Convert.ChangeType(value, valueType);
					
					if (nodeTree.Root == null)
					{
						//go to .bin file directly, it's ONE page of items
						collection = Column.IndexItems<T>()
							.FindByOper(DbGenerator.ItemsPageStart, key, Expression.Operator.Token);
					}
					else
					{
						//this's for tree node page structure
						switch (Expression.Operator.Token)
						{
							case TokenType.Equal: // "="
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
										DbKeyValues<T> item = Column.IndexItems<T>().Find(offset, key);
										if (item != null && item.Values != null)
										{
											collection = item.Values;
										}
										break;
								}
								break;
							case TokenType.Less: // "<"
								collection = nodeTree.FindLessThanKey(key);
								break;
							case TokenType.LessOrEqual: // "<="
								collection = nodeTree.FindLessOrEqualThanKey(key);
								break;
							case TokenType.Greater: // ">"
								collection = nodeTree.FindGreaterThanKey(key);
								break;
							case TokenType.GreaterOrEqual:  //">="
								collection = nodeTree.FindGreaterOrEqualThanKey(key);
								break;
								//throw new ArgumentException($"Operator: {Expression.Operator.Name} not implemented yet!");
						}
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
