using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;
using System.Reflection;

namespace CsvDb
{
	public class CsvDbQuery
	{
		public CsvDb Database { get; protected internal set; }

		public string Query { get; protected internal set; }

		public CsvDbTable Table { get; protected internal set; }

		public CsvDbQueryColumnSelector Columns { get; protected internal set; }

		/// <summary>
		/// -1 is not selected
		/// </summary>
		public int Skip { get; protected internal set; }

		/// <summary>
		/// -1 is not selected
		/// </summary>
		public int Limit { get; protected internal set; }

		public List<CsvDbQueryExpressionBase> Where { get; protected internal set; }

		protected internal CsvDbQuery(
			CsvDb db,
			string query,
			CsvDbTable table,
			CsvDbQueryColumnSelector columns,
			List<CsvDbQueryExpressionBase> where,
			int skip = -1,
			int limit = -1
		)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Database is undefined");
			}
			if ((Table = table) == null || (Columns = columns) == null)
			{
				throw new ArgumentException("Csv Db Query must has a table and a column(s)");
			}
			//ensure always has value
			Where = where ?? new List<CsvDbQueryExpressionBase>();
			Skip = skip <= 0 ? -1 : skip;
			Limit = limit <= 0 ? -1 : limit;
			//	SELECT * FROM [table] t
			//		WHERE t.Name == ""
		}

		public override string ToString()
		{
			var array = new List<string>
			{
				"SELECT",
				Columns.ToString(),
				"FROM",
				Table.Name
			};
			if (Where.Count > 0)
			{
				array.Add($"WHERE {String.Join(" ", Where)}");
			};
			if (Skip > 0)
			{
				array.Add($"SKIP {Skip}");
			}
			if (Limit > 0)
			{
				array.Add($"LIMIT {Limit}");
			}
			return String.Join(" ", array);
		}

		#region Execute

		protected internal IEnumerable<int> Execute()
		{
			//execute where expresion containing the indexed column
			//	 returns a collection of offset:int
			//with that result
			//  according to AND/OR filter by offset values

			//execute all indexers and stack with logicals
			CsvDbQueryLogic oper = CsvDbQueryLogic.Undefined;
			IEnumerable<int> result = null;

			foreach (var item in Where)
			{
				if (item.IsExpression)
				{
					var expr = item as CsvDbQueryExpression;
					var table = expr.Table as CsvDbQueryTableColumnOperand;
					var typeIndex = Type.GetType($"System.{table.Column.Type}");

					var classType = typeof(CsvDbQueryIndexExecuter<>);
					Type genericClass = classType.MakeGenericType(typeIndex);

					var objClass = Activator.CreateInstance(genericClass, new object[] {
						expr
					});
					MethodInfo execute_Method = genericClass.GetMethod("Execute");

					var collection = (IEnumerable<int>)execute_Method.Invoke(objClass, new object[] { });

					if (result == null)
					{
						result = collection;
					}
					else
					{
						//perform logicals
						switch (oper)
						{
							case CsvDbQueryLogic.AND:
								result = result.Intersect(collection);
								break;
							case CsvDbQueryLogic.OR:
								result = result.Union(collection);
								break;
						}
					}
				}
				else
				{
					var logical = item as CsvDbQueryLogical;
					oper = logical.LogicalType;
				}
			}

			foreach (var offset in result)
			{
				yield return offset;
			}
		}
		
		#endregion

		// *\t[table]\t[[column],==,[value]>]
		//SELECT route_id, rout_short_name FROM routes r
		//		WHERE r.agency_id == "NJT" AND
		//					r.route_type == 2
		// route_id,route_short_name\t[routes]\t[agency_id],==,"NJT"\tAND\t[route_type],==,2

		public class CsvDbQueryColumnSelector
		{
			public List<CsvDbColumn> Columns { get; protected internal set; }

			public bool IsFull { get; protected internal set; }

			protected internal CsvDbQueryColumnSelector(List<CsvDbColumn> columns, bool isFull)
			{
				Columns = columns;
				IsFull = isFull;
			}

			public override string ToString() => IsFull ? "*" : String.Join(",", Columns.Select(c => c.Name));
		}

		public enum CsvDbQueryWhereItemType
		{
			/// <summary>
			/// Expression divided into Operand Operator Operand + Logical
			/// </summary>
			Expression,
			/// <summary>
			/// table column, number, string
			/// </summary>
			Operand,
			/// <summary>
			/// equal, greater, greater and equal, less, less and equal, non equal
			/// </summary>
			Operator,
			/// <summary>
			/// AND, OR
			/// </summary>
			Logical
		}

		public abstract class CsvDbQueryWhereItem
		{
			public CsvDbQueryWhereItemType Type { get; protected internal set; }

			protected internal CsvDbQueryWhereItem(CsvDbQueryWhereItemType type)
			{
				Type = type;
			}
		}

		#region Operands

		public abstract class CsvDbQueryOperand : CsvDbQueryWhereItem
		{

			public abstract bool IsTableColumn { get; }

			public abstract string Name { get; }

			public abstract CsvDbColumnTypeEnum OperandType { get; }

			public abstract object Value(ref object[] values);

			public abstract object Value();

			protected internal CsvDbQueryOperand()
				: base(CsvDbQueryWhereItemType.Operand)
			{ }

			public override string ToString() => $"{Name}";

			protected internal static CsvDbQueryOperand Parse(CsvDbTable table, string name)
			{
				//string
				if (name.StartsWith('"') && name.EndsWith('"'))
				{
					return new CsvDbQueryStringOperand(name);
				}
				//now test for table column's name
				var column = table.Columns.FirstOrDefault(c => c.Name == name);
				if (column != null)
				{
					return new CsvDbQueryTableColumnOperand(column, name);
				}
				//	number
				return new CsvDbQueryNumberOperand(name);
			}
		}

		public class CsvDbQueryTableColumnOperand : CsvDbQueryOperand
		{
			public override bool IsTableColumn => true;

			public CsvDbColumn Column { get; protected internal set; }

			public override CsvDbColumnTypeEnum OperandType => Column.TypeEnum;

			public override string Name { get => Column.Name; }

			public override object Value(ref object[] values) { return values[Column.Index]; }

			public override object Value()
			{
				throw new ArgumentException("Table columns need record values referenced");
			}

			protected internal CsvDbQueryTableColumnOperand(CsvDbColumn column, string tableName)
			{
				Column = column;
			}
		}

		public abstract class CsvDbQueryConstantOperand : CsvDbQueryOperand
		{
			// "#.##";
			// "" means full number
			static string format = "";
			//later test for format
			public static string Format { get => format; set => format = value; }
		}

		public class CsvDbQueryStringOperand : CsvDbQueryConstantOperand
		{
			public override bool IsTableColumn => false;

			public override CsvDbColumnTypeEnum OperandType => CsvDbColumnTypeEnum.String;

			string name;
			public override string Name => name;

			public override object Value(ref object[] values) { return name; }

			public override object Value()
			{
				return name;
			}

			public CsvDbQueryStringOperand(string name)
			{
				this.name = name;
			}
		}

		public class CsvDbQueryNumberOperand : CsvDbQueryConstantOperand
		{
			public override bool IsTableColumn => false;

			CsvDbColumnTypeEnum type;
			public override CsvDbColumnTypeEnum OperandType => type;

			public override string Name => _value.ToString();

			object _value;
			public override object Value(ref object[] values) { return _value; }

			public override object Value() { return _value; }

			public T GetValueAs<T>()
			{
				T val = default(T);
				var typeName = val.GetType().Name;
				if (Enum.TryParse<TypeCode>(typeName, out TypeCode typeCode))
				{
					return (T)Convert.ChangeType(_value, typeCode);
				}
				throw new ArgumentException($"Unable to cast operand to T [{typeName}]");
			}

			//name can be a cast like (Byte)8
			public CsvDbQueryNumberOperand(string name)
			{
				if (int.TryParse(name, out int intValue))
				{
					//Byte
					//Int16
					//Int32
					_value = intValue;
					type = CsvDbColumnTypeEnum.Int32;
				}
				else if (Double.TryParse(name, out double doubleValue))
				{
					//Double
					_value = doubleValue;
					type = CsvDbColumnTypeEnum.Double;
				}
				else
				{
					throw new ArgumentException($"Invalid number operand: {name}");
				}
			}

		}//

		#endregion

		#region Conditions supported =, >, >=, <, <=

		public enum CsvDbQueryConditionOper
		{
			Equal,
			NotEqual,
			Greater,
			GreaterOrEqual,
			Less,
			LessOrEqual
		}

		public class CsvDbQueryOperator : CsvDbQueryWhereItem
		{
			public CsvDbQueryConditionOper OperatorType { get; protected internal set; }

			public string Name { get; protected internal set; }

			protected internal CsvDbQueryOperator(string oper)
				: base(CsvDbQueryWhereItemType.Operator)
			{
				switch (Name = oper)
				{
					case "=":
						OperatorType = CsvDbQueryConditionOper.Equal;
						break;
					case "<>":
						OperatorType = CsvDbQueryConditionOper.NotEqual;
						break;
					case ">":
						OperatorType = CsvDbQueryConditionOper.Greater;
						break;
					case ">=":
						OperatorType = CsvDbQueryConditionOper.GreaterOrEqual;
						break;
					case "<":
						OperatorType = CsvDbQueryConditionOper.Less;
						break;
					case "<=":
						OperatorType = CsvDbQueryConditionOper.LessOrEqual;
						break;
					default:
						throw new ArgumentException($"Invalid condition operator [{oper}]");
				}
			}

			public override string ToString() => $"{Name}";
		}

		#endregion

		#region Expression

		public abstract class CsvDbQueryExpressionBase : CsvDbQueryWhereItem
		{
			public abstract bool IsExpression { get; }

			public CsvDbQueryExpressionBase()
				: base(CsvDbQueryWhereItemType.Expression)
			{ }
		}

		public class CsvDbQueryExpression : CsvDbQueryExpressionBase
		{

			public override bool IsExpression => true;

			public CsvDbQueryOperand Left { get; protected internal set; }

			public CsvDbQueryOperator Operator { get; protected internal set; }

			public CsvDbQueryOperand Right { get; protected internal set; }

			public CsvDbQueryOperand Table
			{
				get
				{
					if (Left.IsTableColumn)
					{
						return Left;
					}
					else if (Right.IsTableColumn)
					{
						return Right;
					}
					return null;
				}
			}

			protected internal CsvDbQueryExpression(CsvDbTable table, string left, string oper, string right)
			{
				if ((Left = CsvDbQueryOperand.Parse(table, left)) == null ||
					(Right = CsvDbQueryOperand.Parse(table, right)) == null)
				{
					throw new ArgumentException($"Database query expression invalid");
				}
				//test types

				Operator = new CsvDbQueryOperator(oper);
			}

			public bool Evaluate(ref object[] values)
			{
				var l = Left.Value(ref values);
				var r = Right.Value(ref values);


				return false;
			}

			bool Comparer<T>(T left, CsvDbQueryConditionOper comp, T right)
				where T : IComparable<T>
			{
				var result = left.CompareTo(right);
				switch (Operator.OperatorType)
				{
					case CsvDbQueryConditionOper.Equal:
						return result == 0;
					case CsvDbQueryConditionOper.NotEqual:
						return result != 0;
					case CsvDbQueryConditionOper.Greater:
						return result > 0;
					case CsvDbQueryConditionOper.GreaterOrEqual:
						return result >= 0;
					case CsvDbQueryConditionOper.Less:
						return result < 0;
					case CsvDbQueryConditionOper.LessOrEqual:
						return result <= 0;
				}
				return false;
			}

			public override string ToString() => $"{Left} {Operator} {Right}";
		}

		#endregion

		#region Logical Operands supported AND, OR

		public enum CsvDbQueryLogic
		{
			Undefined,
			AND,
			OR
		}

		public class CsvDbQueryLogical : CsvDbQueryExpressionBase
		{
			public override bool IsExpression => false;

			public CsvDbQueryLogic LogicalType { get; protected internal set; }

			public string Name { get; protected internal set; }

			protected internal CsvDbQueryLogical(string condition)
			{
				if (String.IsNullOrWhiteSpace(condition))
				{
					throw new ArgumentException($"Invalid condition, cannot be null or empty");
				}
				switch (Name = condition.ToUpper())
				{
					case "AND":
						LogicalType = CsvDbQueryLogic.AND;
						break;
					case "OR":
						LogicalType = CsvDbQueryLogic.OR;
						break;
					default:
						throw new ArgumentException($"Invalid condition [{Name}]");
				}
			}

			public override string ToString() => $"{Name}";
		}

		#endregion

		#region Parse

		//make a real parser later to include parenthesis, real type cast,...

		static string pattern =
			@"\s*SELECT\s+(?<columns>\*|(?:\w+\s*,\s*)*\w+)" +
			@"\s+FROM\s+(?<table>\w+)" +
			//"(?<where>\\s*WHERE\\s*(?:(?:(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\"))\\s*(?:=|<>|[><]=?)\\s*(?:(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\"))))\\s*(?:AND|OR)?\\s*)*)?" +
			"(?<where>\\s*WHERE\\s*(?:(?:(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))\\s*(?:=|<>|[><]=?)\\s*(?:(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))\\s*(?:AND|OR)?\\s*)*)" +
			//with casting (Byte)5
			//"(?<where>\\s*WHERE\\s*(?:(?:(?:\\(\\w+\\))?(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))\\s*(?:=|<>|[><]=?)\\s*(?:(?:\\(\\w+\\))?(?:(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))\\s*(?:AND|OR)?\\s*)*)" +
			"(?:\\s*SKIP\\s+(?<skip>\\d+))?" +
			"(?:\\s*LIMIT\\s+(?<limit>\\d+))?";

		static Regex patterRegEx = new Regex(pattern, RegexOptions.Multiline | RegexOptions.Compiled);

		static string paramPattern =
			"(?:(?<left>(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\"))\\s*(?<oper>=|<>|[><]=?)\\s*(?<right>(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))|(?<cond>AND|OR)";
		//"(?:(?<left>(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\"))\\s*(?<oper>=|<>|[><]=?)\\s*(?<right>(?:[-+]?\\d+\\.?\\d+)|\\w+|(?:\"[^\"]+\")))|(?<cond>AND|OR)";

		static Regex regexWhere = new Regex(paramPattern, RegexOptions.Multiline | RegexOptions.Compiled);

		//https://regex101.com/

		public static CsvDbQuery Parse(CsvDb db, string query)
		{
			var match = patterRegEx.Match(query);
			if (!match.Success)
			{
				throw new ArgumentException($"Invalid query against database.");
			}

			//Table
			var table = db.Table(match.Groups["table"].Value.Trim());
			if (table == null)
			{
				throw new ArgumentException($"Cannot find table name of database.");
			}

			//Columns
			var group = match.Groups["columns"];
			var columns = new List<CsvDbColumn>();
			var isFull = false;
			if (group.Value == "*")
			{
				isFull = true;
				columns = table.Columns;
			}
			else
			{
				foreach (var colName in group.Value.Split(","))
				{
					var c = table.Column(colName.Trim());
					if (c == null)
					{
						throw new ArgumentException($"Cannot find {colName} in [{table.Name}] name of database.");
					}
					columns.Add(c);
				}
			}
			var colSelector = new CsvDbQueryColumnSelector(columns, isFull);

			//Skip
			int skip = -1;
			if ((group = match.Groups["skip"]).Success &&
				(!Int32.TryParse(group.Value, out skip) ||
				skip <= 0))
			{
				throw new ArgumentException($"SKIP must be a positive integer greater than zero.");
			}

			//Limit
			int limit = -1;
			if ((group = match.Groups["limit"]).Success &&
				(!Int32.TryParse(group.Value, out limit) ||
				limit <= 0))
			{
				throw new ArgumentException($"LIMIT must be a positive integer greater than zero.");
			}

			//Where
			var where = new List<CsvDbQueryExpressionBase>();
			if ((group = match.Groups["where"]).Success)
			{
				var matchesWhere = regexWhere.Matches(group.Value);
				if (matchesWhere == null)
				{
					throw new ArgumentException($"Invalid WHERE query against database.");
				}
				foreach (Match m in matchesWhere)
				{
					CsvDbQueryExpressionBase expr = null;
					if (m.Groups["cond"].Success)
					{
						expr = new CsvDbQueryLogical(m.Groups["cond"].Value);
					}
					else
					{
						expr = new CsvDbQueryExpression(
							table,
							m.Groups["left"].Value,
							m.Groups["oper"].Value,
							m.Groups["right"].Value
						);
					}
					if (expr == null)
					{
						throw new ArgumentException($"Invalid WHERE query against database.");
					}
					where.Add(expr);
				}
			}
			return new CsvDbQuery(db, query, table, colSelector, where, skip, limit);
		}

		#endregion

	}
}
