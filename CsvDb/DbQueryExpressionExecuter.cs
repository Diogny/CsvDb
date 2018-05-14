using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// Executes an sql simple expression col0 > 2. Restriction to column & constant and viceversa only
	/// </summary>
	/// <typeparam name="T">type of column</typeparam>
	internal class DbQueryExpressionExecuter<T>
		 where T : IComparable<T>
	{
		readonly DbQueryHandler Handler;

		//public DbQuery.ComparisonOperator Expression { get; }

		public DbColumn Column { get; }

		public TokenType Operator { get; }

		public DbQuery.ExpressionValue Constant { get; }

		public DbTableDataReader Reader { get; }

		internal DbQueryExpressionExecuter(DbQueryHandler handler,
			DbQuery.ColumnOperand columnOperand, TokenType oper, DbQuery.ConstantOperand constantOperand)
		{
			if (columnOperand == null || !(Operator = oper).IsComparison() || constantOperand == null ||
				(Handler = handler) == null)
			{
				throw new ArgumentException($"cannot execute comparison expression");
			}

			//get the table column handler
			Column = handler.Database.Index(columnOperand.Column.Hash);
			//  . handler.WhereColumns[columnOperand.Column.Hash];

			//get the constant value to compare
			Constant = constantOperand.Evaluate(null);

			Reader = Handler.TableReaders.FirstOrDefault(r => r.Table == Column.Table);
			if (Reader == null)
			{
				throw new ArgumentException($"could not resolve data table reader for column: {Column}");
			}
		}

		//we need key to look into rows without index columns
		public IEnumerable<KeyValuePair<T, List<int>>> Execute()
		{
			//later allow use of non-indexed columns here: DbTableDataReader can handle it

			var constantValue = Constant.ValueAs<T>();

			var key = Column.Table.Columns.FirstOrDefault(c => c.Key);

			if (Column.Indexed)
			{
				return Reader.CompareIndexedKeyWithKey<T>(key, Column, Operator, constantValue);
			}
			else
			{
				throw new ArgumentException($"cannot evaluate no indexed column: {Column}");

			}

			//var collection = Enumerable.Empty<KeyValuePair<T, List<int>>>();

			//var nodeTree = Column.IndexTree<T>();
			//int offset = -1;

			//var constantValue = Constant.ValueAs<T>();

			//if (nodeTree.Root == null)
			//{
			//	//go to .bin file directly, it's ONE page of items
			//	collection = Column.IndexItems<T>()
			//		.FindByOper(DbGenerator.ItemsPageStart, constantValue, Operator);
			//}
			//else
			//{
			//	//this's for tree node page structure
			//	switch (Operator)
			//	{
			//		case TokenType.Equal: // "="
			//													//find page with key
			//			var baseNode = nodeTree.FindKey(constantValue) as PageIndexNodeBase<T>;
			//			if (baseNode != null)
			//			{
			//				switch (baseNode.Type)
			//				{
			//					case MetaIndexType.Node:
			//						//it's in a tree node page
			//						var nodePage = baseNode as PageIndexNode<T>;
			//						if (nodePage != null && nodePage.Values != null)
			//						{
			//							collection = new KeyValuePair<T, List<int>>[] {
			//								new KeyValuePair<T,List<int>>(constantValue, nodePage.Values)
			//							};
			//						}
			//						break;
			//					case MetaIndexType.Items:
			//						//it's in a items page 
			//						offset = ((PageIndexItems<T>)baseNode).Offset;
			//						DbKeyValues<T> item = Column.IndexItems<T>().Find(offset, constantValue);
			//						if (item != null && item.Values != null)
			//						{
			//							collection = new KeyValuePair<T, List<int>>[] {
			//								new KeyValuePair<T,List<int>>(constantValue, item.Values)
			//							};
			//						}
			//						break;
			//				}
			//			}
			//			break;
			//		case TokenType.Less: // "<"
			//			collection = nodeTree.FindLessKey(constantValue, TokenType.Less);
			//			break;
			//		case TokenType.LessOrEqual: // "<="
			//			collection = nodeTree.FindLessKey(constantValue, TokenType.LessOrEqual);
			//			break;
			//		case TokenType.Greater: // ">"
			//			collection = nodeTree.FindGreaterKey(constantValue, TokenType.Greater);
			//			break;
			//		case TokenType.GreaterOrEqual:  //">="
			//			collection = nodeTree.FindGreaterKey(constantValue, TokenType.GreaterOrEqual);
			//			break;
			//			//throw new ArgumentException($"Operator: {Expression.Operator.Name} not implemented yet!");
			//	}
			//}

			//foreach (var ofs in collection)
			//{
			//	yield return ofs;
			//}
		}
	}
}
