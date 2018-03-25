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

		public CsvDbQueryTableIdentifier TableIdentifier { get; protected internal set; }

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
			CsvDbQueryTableIdentifier table,
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
			if ((TableIdentifier = table) == null || (Columns = columns) == null)
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
				TableIdentifier.ToString()
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
			var executerClassType = typeof(CsvDbQueryIndexExecuter<>);

			if (Where.Count == 0)
			{
				//find key column
				var keyColumn = TableIdentifier.Table.Columns.FirstOrDefault(c => c.Key);
				if (keyColumn == null)
				{
					throw new ArgumentException($"Cannot find key from table [{TableIdentifier.Table.Name}]");
				}
				var typeIndex = Type.GetType($"System.{keyColumn.Type}");

				Type genericClass = executerClassType.MakeGenericType(typeIndex);

				var objClass = Activator.CreateInstance(genericClass, new object[] {
							keyColumn
						});
				MethodInfo execute_Method = genericClass.GetMethod("Execute");

				result = (IEnumerable<int>)execute_Method.Invoke(objClass, new object[] { });
			}
			else
			{
				foreach (var item in Where)
				{
					if (item.IsExpression)
					{
						var expr = item as CsvDbQueryExpression;
						var table = expr.Table as CsvDbQueryTableColumnOperand;
						var typeIndex = Type.GetType($"System.{table.Column.Type}");

						Type genericClass = executerClassType.MakeGenericType(typeIndex);

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
			public List<CsvDbQueryColumnIdentifier> Columns { get; protected internal set; }

			public bool IsFull { get; protected internal set; }

			internal CsvDbQueryColumnSelector(List<CsvDbQueryColumnIdentifier> columns, bool isFull)
			{
				Columns = columns;
				IsFull = isFull;
			}

			public string[] Header => Columns.Select(c => c.Column.Name).ToArray();

			public override string ToString() => IsFull ? "*" : String.Join(",", Columns.Select(c => c.ToString()));
		}

		public interface INameIdentifier
		{
			string Identifier { get; }

			bool HasIdentifier { get; }

		}

		public abstract class NameIdentifier : INameIdentifier
		{
			public string Identifier { get; private set; }

			public bool HasIdentifier => Identifier != null;

			public NameIdentifier(string identifier)
			{
				//normalize lowercase
				Identifier = String.IsNullOrWhiteSpace(identifier) ? null : identifier.Trim().ToLower();
			}
		}

		public class CsvDbQueryTableIdentifier : NameIdentifier
		{
			public CsvDbTable Table { get; private set; }

			internal CsvDbQueryTableIdentifier(CsvDbTable table, string identifier = null)
				: base(identifier)
			{
				//identifier cannot match any column inside any table in database
				if ((Table = table) == null ||
					(Identifier != null &&
						table.Database.Tables
							.SelectMany(t => t.Columns)
							.Any(c => String.Compare(c.Name, Identifier, true) == 0)))
				{
					throw new ArgumentException("table does not exists or identifier match one of its columns");
				}
			}

			public override string ToString() => $"{Table.Name}" + (HasIdentifier ? $" {Identifier}" : "");

		}

		public class CsvDbQueryColumnIdentifier : NameIdentifier
		{
			public CsvDbColumn Column { get; private set; }

			internal CsvDbQueryColumnIdentifier(CsvDbColumn column, string identifier = null)
				: base(identifier)
			{
				if ((Column = column) == null)
				{
					throw new ArgumentException("column identifier cannot have an undefined column");
				}
			}

			public override string ToString() => $"{(HasIdentifier ? $"{Identifier}." : "")}{Column.Name}";
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

			internal CsvDbQueryWhereItem(CsvDbQueryWhereItemType type)
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

			//later table will be a collection of table with more complex queries
			//  because [identifier].[column] could point to more than one table
			internal static CsvDbQueryOperand Parse(IEnumerable<CsvDbQueryTableIdentifier> tables,
				CsvDbQueryParser.TokenStruct token)
			{
				switch (token.Token)
				{
					case CsvDbQueryParser.Token.String:
						return new CsvDbQueryStringOperand(token.Value);
					case CsvDbQueryParser.Token.Number:
						return new CsvDbQueryNumberOperand(token.Value);
					case CsvDbQueryParser.Token.Identifier:
						//now test for table column's name
						var column = tables
							.SelectMany(t => t.Table.Columns)
							.FirstOrDefault(c => c.Name == token.Value);
						if (column != null)
						{
							return new CsvDbQueryTableColumnOperand(column);
						}
						break;
					case CsvDbQueryParser.Token.ColumnIdentifier:
						var pars = token.Value.Split('.');
						//find the table by its identifier if any
						var tble = tables
							.Where(t => t.HasIdentifier)
							.FirstOrDefault(t => t.Identifier == pars[0]);
						//find the column inside the table
						column = tble?.Table.Columns.FirstOrDefault(c => c.Name == pars[1]);
						if (column != null)
						{
							return new CsvDbQueryTableColumnOperand(column, pars[0]);
						}
						break;
				}
				throw new ArgumentException($"{token.Value} doesnot exist in any table");
			}

			//later table will be a collection of table with more complex queries
			//  because [identifier].[column] could point to more than one table
			internal static CsvDbQueryOperand Parse(CsvDbTable table, string name)
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
					return new CsvDbQueryTableColumnOperand(column);
				}
				//if it doesn't start with [0-9] or [+-] then bad column
				if (!(char.IsDigit(name[0]) || name[0] == '+' || name[0] == '-'))
				{
					throw new ArgumentException($"[{table.Name}].{name} column not found in database!");
				}
				//	number
				return new CsvDbQueryNumberOperand(name);
			}
		}

		public class CsvDbQueryTableColumnOperand : CsvDbQueryOperand
		{
			public override bool IsTableColumn => true;

			public CsvDbColumn Column { get; private set; }

			public override CsvDbColumnTypeEnum OperandType => Column.TypeEnum;

			public override string Name { get => Column.Name; }

			public override object Value(ref object[] values) { return values[Column.Index]; }

			/// <summary>
			/// Table identitifier
			/// </summary>
			public string Identifier { get; private set; }

			/// <summary>
			/// Has table identifier
			/// </summary>
			public bool HasIdentifier { get; private set; }

			public override object Value()
			{
				throw new ArgumentException("Table columns need record values referenced");
			}

			internal CsvDbQueryTableColumnOperand(CsvDbColumn column, string tableIdentifier = null)
			{
				Column = column;
				HasIdentifier = (Identifier = String.IsNullOrWhiteSpace(tableIdentifier) ?
					null :
					tableIdentifier.Trim()
				) != null;
			}

			public override string ToString() => $"{(HasIdentifier ? $"{Identifier}." : "")}{Name}";
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

			public override string ToString() => $"\"{Name}\"";

			internal CsvDbQueryStringOperand(string name)
			{
				//remove the enclosing "" from string

				this.name = name.Substring(1, name.Length - 2); // name;
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
			internal CsvDbQueryNumberOperand(string name)
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

		}

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

			protected internal CsvDbQueryExpression(IEnumerable<CsvDbQueryTableIdentifier> tables,
				CsvDbQueryParser.TokenStruct left,
				string oper,
				CsvDbQueryParser.TokenStruct right)
			{
				if ((Left = CsvDbQueryOperand.Parse(tables, left)) == null ||
					(Right = CsvDbQueryOperand.Parse(tables, right)) == null)
				{
					throw new ArgumentException($"Database query expression invalid");
				}
				//test types

				Operator = new CsvDbQueryOperator(oper);
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

	}
}
