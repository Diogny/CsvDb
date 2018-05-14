using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;

namespace CsvDb
{

	/// <summary>
	/// parallel class to develop multi-table.column handling in WHERE
	/// </summary>
	public class DbQueryHandler : IDisposable
	{
		/// <summary>
		/// database
		/// </summary>
		public CsvDb Database { get; }

		/// <summary>
		/// parsed sql query
		/// </summary>
		public DbQuery Query { get; }

		/// <summary>
		/// database table data reader collection
		/// </summary>
		public List<DbTableDataReader> TableReaders { get; }

		/// <summary>
		/// row/record count
		/// </summary>
		public int RowCount { get; private set; }

		internal SelectColumnHandler SelectIndexColumns;

		/// <summary>
		/// returns the real SELECT row column count
		/// </summary>
		public int ColumnCount => SelectIndexColumns.Count;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="db"></param>
		/// <param name="query"></param>
		public DbQueryHandler(CsvDb db, DbQuery query)
		{
			if ((Database = db) == null ||
				(Query = query) == null)
			{
				throw new ArgumentException("Cannot create query executer handler");
			}

			//set index transformers to table columns
			SelectIndexColumns = new SelectColumnHandler(query.Select.Columns);

			//WhereColumns = new Dictionary<string, DbColumn>();
			TableReaders = new List<DbTableDataReader>();

			//get table data reader from FROM tables too, in case WHERE has no column
			foreach (var table in query.From)
			{
				if (!TableReaders.Any(t => t.Table.Name == table.Name))
				{
					TableReaders.Add(DbTableDataReader.Create(db, table.Name, this));
				}
			}

		}

		public void Dispose()
		{
			foreach (var dbreader in TableReaders)
			{
				dbreader.Dispose();
			}
			TableReaders.Clear();
		}

		/// <summary>
		/// execute query to get result rows
		/// </summary>
		/// <returns></returns>
		public IEnumerable<object[]> Rows()
		{
			//only for one table for now
			var table = Query.Tables.First();
			var handler = TableReaders.FirstOrDefault(r => r.Table.Name == table.Name);

			var collection = Enumerable.Empty<int>();

			if (!Query.Where.Defined)
			{
				//no WHERE, ALL table
				//return the full table rows

				//later mix all tables according to FROM tables and SELECT rows


				var type = handler.GetType();
				var method = type.GetMethod(nameof(DbTableDataReader.Rows),
					BindingFlags.Instance | BindingFlags.NonPublic);

				MethodInfo genMethod = method.MakeGenericMethod(handler.Table.Type);
				//get first key
				var key = handler.Table.Columns.FirstOrDefault(c => c.Key);

				collection = (IEnumerable<int>)genMethod.Invoke(handler, new object[] { key });
			}
			else
			{
				//filtered table
				collection = ExecuteWhere(Query.Where.Root as DbQuery.ExpressionOperator);
			}

			//TOP
			if (Query.Select.Top > 0)
			{
				collection = collection.Take(Query.Select.Top);
			}

			//process collection of offsets
			if (Query.Select.IsFunction)
			{
				RowCount = 1;
				//query functions contains only one column in SELECT
				var column = SelectIndexColumns.Columns.First();
				var valueType = column.Type;

				if (Query.Select.Function == TokenType.COUNT)
				{
					//Enumerable.Count() returns an int
					yield return new object[] { collection.Count() };
				}
				else
				{
					if (!valueType.IsNumeric())
					{
						throw new ArgumentException($"function {Query.Select.Function} column type must be numeric");
					}
					MethodInfo method =
					this.GetType().GetMethod(nameof(ExecuteFunction), BindingFlags.Instance | BindingFlags.NonPublic);

					MethodInfo genMethod = method.MakeGenericMethod(Type.GetType($"System.{valueType}"));

					var result = genMethod.Invoke(this, new object[] { handler, valueType, collection });

					yield return new object[] { result };
				}
			}
			else
			{
				//output all rows filtered or not
				RowCount = 0;
				foreach (var offset in collection)
				{
					RowCount++;
					yield return handler.ReadRecord(offset);
				}
			}
		}

		internal T ExecuteFunction<T>(DbTableDataReader reader,
			DbColumnType valueType,
			IEnumerable<int> collection)
		{
			dynamic oneColumnCollection = collection;

			//apply quantifiers here
			if (Query.Select.Function != TokenType.COUNT)
			{
				//get real values

				//not transformed, need to
				var recordValues = new List<object[]>();
				foreach (var offset in collection)
				{
					recordValues.Add(reader.ReadRecord(offset));
				}
				//read records return only one column
				oneColumnCollection = recordValues.Select(cols => cols[0]).Cast<T>();
			}

			MethodInfo method =
				this.GetType().GetMethod(nameof(ApplyFunction), BindingFlags.Instance | BindingFlags.NonPublic);

			MethodInfo genMethod = method.MakeGenericMethod(Type.GetType($"System.{valueType}"));

			var result = genMethod.Invoke(this, new object[] { oneColumnCollection, Query.Select.Function });

			return (T)result;
		}

		//		Expression<Func<IEnumerable<T>, T>> CreateLambda<T>(string function)
		//		{
		//			var source = Expression.Parameter(
		//					typeof(IEnumerable<T>), "source");

		//			var p = Expression.Parameter(typeof(T), "p");

		//			MethodCallExpression call = Expression.Call(
		//						typeof(Enumerable), function, new Type[] { typeof(T) }, source);

		//			return Expression.Lambda<Func<IEnumerable<T>, T>>(call, source);
		//		}

		internal T ApplyFunction<T>(IEnumerable<T> valueCollection, TokenType function)
		{
			switch (function)
			{
				//case TokenType.COUNT:
				//	var count = valueCollection.Count();
				//	return (T)Convert.ChangeType(count, typeof(T));
				case TokenType.AVG:
					var avg = valueCollection.Average(x => (dynamic)x);
					return (T)Convert.ChangeType(avg, typeof(T));
				case TokenType.SUM:
					var sum = valueCollection.Sum(x => (dynamic)x);
					return (T)Convert.ChangeType(sum, typeof(T));
				default:
					throw new ArgumentException($"invalid quantifier {function} in query");
			}
			//var lambda = CreateLambda<T>(functionName);
			//var result = lambda.Compile().Invoke(valueCollection);

			//return result;
		}

		IEnumerable<int> ExecuteWhere(DbQuery.ExpressionOperator root)
		{
			var result = Enumerable.Empty<int>();

			if (root.Operator.IsComparison())
			{
				//call DbQueryExpressionExecuter
				if (!root.TrySplitColumnConstant(out DbQuery.ColumnOperand columnOperand,
					out DbQuery.ConstantOperand constantOperand))
				{
					throw new ArgumentException("cannot execute WHERE conditions");
				}
				var type = System.Type.GetType($"System.{columnOperand.Type}");

				MethodInfo method =
					this.GetType().GetMethod(nameof(DbQueryHandler.ExecuteColumnExpression),
					BindingFlags.Instance | BindingFlags.NonPublic);

				MethodInfo genMethod = method.MakeGenericMethod(type);

				//should record which table gave this result of offset rows

				result = (IEnumerable<int>)genMethod.Invoke(this,
					new object[] { this, columnOperand, root.Operator, constantOperand });
			}
			else
			{
				//should be a logical operator
				var left = ExecuteWhere(root.Left as DbQuery.ExpressionOperator);
				var right = ExecuteWhere(root.Right as DbQuery.ExpressionOperator);
				//
				switch (root.Operator)
				{
					//this happens if left and right share the same table
					// if not it's a multi-table

					case TokenType.AND:
						result = left.Intersect(right);
						break;
					case TokenType.OR:
						result = left.Union(right);
						break;
					default:
						throw new ArgumentException($"expected logical operator and we got: {root.Operator}");
				}
			}
			return result;
		}

		/// <summary>
		/// call generic class
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="handler"></param>
		/// <param name="columnOperand"></param>
		/// <param name="oper"></param>
		/// <param name="constantOperand"></param>
		/// <returns></returns>
		IEnumerable<int> ExecuteColumnExpression<T>(DbQueryHandler handler,
			DbQuery.ColumnOperand columnOperand, TokenType oper, DbQuery.ConstantOperand constantOperand)
			where T : IComparable<T>
		{
			var executer = new DbQueryExpressionExecuter<T>(handler, columnOperand, oper, constantOperand);

			return executer.Execute().SelectMany(pair => pair.Value);
		}

	}

	/// <summary>
	/// Implements a SELECT column
	/// </summary>
	public class SelectColumn
	{
		/// <summary>
		/// table column
		/// </summary>
		public DbQuery.Column Column { get; }

		/// <summary>
		/// Column index inside table
		/// </summary>
		public int ColumnIndex { get; }

		/// <summary>
		/// Real output index
		/// </summary>
		public int Index { get; }

		/// <summary>
		/// type of column
		/// </summary>
		public DbColumnType Type { get; }

		public SelectColumn(DbQuery.Column column, int index)
		{
			Column = column;
			ColumnIndex = column.Meta.Index;
			Type = Enum.Parse<DbColumnType>(column.Meta.Type);
			Index = index;
		}

		public override string ToString() => $"[{ColumnIndex} {Column}";
	}

	/// <summary>
	/// Implements a query SELECT column handler
	/// </summary>
	public class SelectColumnHandler
	{
		Dictionary<string, SelectColumn> _columns;

		/// <summary>
		/// Gets the columns in the SELECT clause
		/// </summary>
		public IEnumerable<SelectColumn> Columns => _columns.Values;

		public int Count => _columns.Count;

		/// <summary>
		/// indexer for SELECT column
		/// </summary>
		/// <param name="hash">column hash: table.column</param>
		/// <returns></returns>
		public SelectColumn this[string hash] => _columns.TryGetValue(hash, out SelectColumn col) ? col : null;

		public SelectColumnHandler(IEnumerable<DbQuery.Column> columnCollection)
		{
			_columns = columnCollection
				.Select((col, ndx) => new SelectColumn(col, ndx))
				.ToDictionary(p => p.Column.Hash);
		}
	}
}