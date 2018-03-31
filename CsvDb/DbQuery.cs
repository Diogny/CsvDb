using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;

namespace CsvDb.Query
{
	public class DbQuery
	{
		public CsvDb Database { get; protected internal set; }

		public string Query { get; protected internal set; }

		/// <summary>
		/// Table definition after FROM
		/// </summary>
		public DbQueryTableIdentifier FromTableIdentifier { get; protected internal set; }

		public List<DbQueryTableIdentifier> TablesUsed { get; protected internal set; }

		public DbQueryColumnSelector Columns { get; protected internal set; }

		public SqlJoin Join { get; protected internal set; }

		public List<DbQueryExpressionBase> Where { get; protected internal set; }

		/// <summary>
		/// -1 is not selected
		/// </summary>
		public int Skip { get; protected internal set; }

		/// <summary>
		/// -1 is not selected
		/// </summary>
		public int Limit { get; protected internal set; }

		internal DbQuery(
			CsvDb db,
			string query,
			DbQueryColumnSelector columns,
			DbQueryTableIdentifier table,
			List<DbQueryTableIdentifier> usedTables,
			SqlJoin join = null,
			List<DbQueryExpressionBase> where = null,
			int skip = -1,
			int limit = -1
		)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Database is undefined");
			}
			if ((FromTableIdentifier = table) == null || (Columns = columns) == null)
			{
				throw new ArgumentException("Csv Db Query must has a table and a column(s)");
			}
			if ((TablesUsed = usedTables) == null || usedTables.Count == 0)
			{
				throw new ArgumentException("Query must use at least one database table");
			}

			Join = join;

			//ensure always has value
			Where = where ?? new List<DbQueryExpressionBase>();
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
				FromTableIdentifier.ToString()
			};
			if (Join != null)
			{
				array.Add(Join.ToString());
			}
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
			DbQueryLogic oper = DbQueryLogic.Undefined;
			IEnumerable<int> result = null;
			var executerClassType = typeof(DbQueryExecuter<>);

			if (Where.Count == 0)
			{
				//find key column
				var keyColumn = FromTableIdentifier.Table.Columns.FirstOrDefault(c => c.Key);
				if (keyColumn == null)
				{
					throw new ArgumentException($"Cannot find key from table [{FromTableIdentifier.Table.Name}]");
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
						var expr = item as DbQueryExpression;
						var table = expr.Table as DbQueryTableColumnOperand;
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
								case DbQueryLogic.AND:
									result = result.Intersect(collection);
									break;
								case DbQueryLogic.OR:
									result = result.Union(collection);
									break;
							}
						}
					}
					else
					{
						var logical = item as DbQueryLogical;
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

	}

	public class SqlJoin
	{
		public Token Type { get; private set; }

		public DbQueryTableIdentifier Table { get; private set; }

		public DbQueryExpression Expression { get; private set; }

		internal SqlJoin(Token type, DbQueryTableIdentifier table, DbQueryExpression expression)
		{
			if (!DbQueryParser.JoinStarts.Contains(Type = type) || (Table = table) == null || (Expression = expression) == null)
			{
				throw new ArgumentException("Invalid SQL JOIN");
			}
		}

		public override string ToString()
		{
			var sb = new StringBuilder();
			sb.Append(Type.ToString());
			if (Type == Token.LEFT || Type == Token.RIGHT || Type == Token.FULL)
			{
				sb.Append(" OUTER");
			}
			sb.Append(" JOIN");
			sb.Append($" {Table.ToString()}");
			sb.Append($" ON {Expression.ToString()}");
			return sb.ToString();
		}
	}


	public class DbQueryColumnSelector
	{
		public List<DbQueryColumnIdentifier> Columns { get; protected internal set; }

		public bool IsFull { get; protected internal set; }

		internal DbQueryColumnSelector(List<DbQueryColumnIdentifier> columns, bool isFull)
		{
			Columns = columns;
			IsFull = isFull;
		}

		public string[] Header => Columns.Select(c => c.Column.Name).ToArray();

		public override string ToString() => IsFull ? "*" : String.Join(",", Columns.Select(c => c.ToString()));
	}

	#region INameIdentifier

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

	public class DbQueryTableIdentifier : NameIdentifier
	{
		public DbTable Table { get; private set; }

		internal DbQueryTableIdentifier(DbTable table, string identifier = null)
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

	public class DbQueryColumnIdentifier : NameIdentifier
	{
		public DbColumn Column { get; private set; }

		internal DbQueryColumnIdentifier(DbColumn column, string identifier = null)
			: base(identifier)
		{
			if ((Column = column) == null)
			{
				throw new ArgumentException("column identifier cannot have an undefined column");
			}
		}

		public override string ToString() => $"{(HasIdentifier ? $"{Identifier}." : "")}{Column.Name}";
	}

	#endregion

	public enum DbQueryExpressionItemType
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

	public abstract class DbQueryExpressionItem
	{
		public DbQueryExpressionItemType Type { get; protected internal set; }

		internal DbQueryExpressionItem(DbQueryExpressionItemType type)
		{
			Type = type;
		}
	}

	#region Operands

	public abstract class DbQueryOperand : DbQueryExpressionItem
	{

		public abstract bool IsTableColumn { get; }

		public abstract string Name { get; }

		public abstract DbColumnTypeEnum OperandType { get; }

		public abstract object Value(ref object[] values);

		public abstract object Value();

		protected internal DbQueryOperand()
			: base(DbQueryExpressionItemType.Operand)
		{ }

		public override string ToString() => $"{Name}";

		//later table will be a collection of table with more complex queries
		//  because [identifier].[column] could point to more than one table
		internal static DbQueryOperand Parse(IEnumerable<DbQueryTableIdentifier> tables,
			TokenItem token)
		{
			switch (token.Token)
			{
				case Token.String:
					return new DbQueryStringOperand(token.Value);
				case Token.Number:
					return new DbQueryNumberOperand(token.Value);
				case Token.Identifier:
					//now test for table column's name
					var column = tables
						.SelectMany(t => t.Table.Columns)
						.FirstOrDefault(c => c.Name == token.Value);
					if (column != null)
					{
						return new DbQueryTableColumnOperand(column);
					}
					break;
				case Token.ColumnIdentifier:
					var tkIdent = (TokenItemIdentifier)token;
					//find the table by its identifier if any
					var tble = tables
						.Where(t => t.HasIdentifier)
						.FirstOrDefault(t => t.Identifier == tkIdent.Identifier);
					//find the column inside the table
					column = tble?.Table.Columns.FirstOrDefault(c => c.Name == tkIdent.Value);
					if (column != null)
					{
						return new DbQueryTableColumnOperand(column, tkIdent.Identifier);
					}
					break;
			}
			throw new ArgumentException($"{token.Value} doesnot exist in any table");
		}

		//later table will be a collection of table with more complex queries
		//  because [identifier].[column] could point to more than one table
		internal static DbQueryOperand Parse(DbTable table, string name)
		{
			//string
			if (name.StartsWith('"') && name.EndsWith('"'))
			{
				return new DbQueryStringOperand(name);
			}
			//now test for table column's name
			var column = table.Columns.FirstOrDefault(c => c.Name == name);
			if (column != null)
			{
				return new DbQueryTableColumnOperand(column);
			}
			//if it doesn't start with [0-9] or [+-] then bad column
			if (!(char.IsDigit(name[0]) || name[0] == '+' || name[0] == '-'))
			{
				throw new ArgumentException($"[{table.Name}].{name} column not found in database!");
			}
			//	number
			return new DbQueryNumberOperand(name);
		}
	}

	public class DbQueryTableColumnOperand : DbQueryOperand
	{
		public override bool IsTableColumn => true;

		public DbColumn Column { get; private set; }

		public override DbColumnTypeEnum OperandType => Column.TypeEnum;

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

		internal DbQueryTableColumnOperand(DbColumn column, string tableIdentifier = null)
		{
			Column = column;
			HasIdentifier = (Identifier = String.IsNullOrWhiteSpace(tableIdentifier) ?
				null :
				tableIdentifier.Trim()
			) != null;
		}

		public override string ToString() => $"{(HasIdentifier ? $"{Identifier}." : "")}{Name}";
	}

	public abstract class DbQueryConstantOperand : DbQueryOperand
	{
		// "#.##";
		// "" means full number
		static string format = "";
		//later test for format
		public static string Format { get => format; set => format = value; }
	}

	public class DbQueryStringOperand : DbQueryConstantOperand
	{
		public override bool IsTableColumn => false;

		public override DbColumnTypeEnum OperandType => DbColumnTypeEnum.String;

		string name;
		public override string Name => name;

		public override object Value(ref object[] values) { return name; }

		public override object Value()
		{
			return name;
		}

		public override string ToString() => $"\"{Name}\"";

		internal DbQueryStringOperand(string name)
		{
			//remove the enclosing "" from string

			this.name = name.Substring(1, name.Length - 2); // name;
		}
	}

	public class DbQueryNumberOperand : DbQueryConstantOperand
	{
		public override bool IsTableColumn => false;

		DbColumnTypeEnum type;
		public override DbColumnTypeEnum OperandType => type;

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
		internal DbQueryNumberOperand(string name)
		{
			if (int.TryParse(name, out int intValue))
			{
				//Byte
				//Int16
				//Int32
				_value = intValue;
				type = DbColumnTypeEnum.Int32;
			}
			else if (Double.TryParse(name, out double doubleValue))
			{
				//Double
				_value = doubleValue;
				type = DbColumnTypeEnum.Double;
			}
			else
			{
				throw new ArgumentException($"Invalid number operand: {name}");
			}
		}

	}

	#endregion

	#region Conditions supported =, >, >=, <, <=

	public enum DbQueryConditionOper
	{
		Equal,
		NotEqual,
		Greater,
		GreaterOrEqual,
		Less,
		LessOrEqual
	}

	public class DbQueryOperator : DbQueryExpressionItem
	{
		public DbQueryConditionOper OperatorType { get; protected internal set; }

		public string Name { get; protected internal set; }

		protected internal DbQueryOperator(string oper)
			: base(DbQueryExpressionItemType.Operator)
		{
			switch (Name = oper)
			{
				case "=":
					OperatorType = DbQueryConditionOper.Equal;
					break;
				case "<>":
					OperatorType = DbQueryConditionOper.NotEqual;
					break;
				case ">":
					OperatorType = DbQueryConditionOper.Greater;
					break;
				case ">=":
					OperatorType = DbQueryConditionOper.GreaterOrEqual;
					break;
				case "<":
					OperatorType = DbQueryConditionOper.Less;
					break;
				case "<=":
					OperatorType = DbQueryConditionOper.LessOrEqual;
					break;
				default:
					throw new ArgumentException($"Invalid condition operator [{oper}]");
			}
		}

		public override string ToString() => $"{Name}";
	}

	#endregion

	#region Expression

	public abstract class DbQueryExpressionBase : DbQueryExpressionItem
	{
		public abstract bool IsExpression { get; }

		public DbQueryExpressionBase()
			: base(DbQueryExpressionItemType.Expression)
		{ }
	}

	public class DbQueryExpression : DbQueryExpressionBase
	{

		public override bool IsExpression => true;

		public DbQueryOperand Left { get; protected internal set; }

		public DbQueryOperator Operator { get; protected internal set; }

		public DbQueryOperand Right { get; protected internal set; }

		public DbQueryOperand Table
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

		protected internal DbQueryExpression(IEnumerable<DbQueryTableIdentifier> tables,
			TokenItem left,
			string oper,
			TokenItem right)
		{
			if ((Left = DbQueryOperand.Parse(tables, left)) == null ||
				(Right = DbQueryOperand.Parse(tables, right)) == null)
			{
				throw new ArgumentException($"Database query expression invalid");
			}
			//test types

			Operator = new DbQueryOperator(oper);
		}

		protected internal DbQueryExpression(DbTable table, string left, string oper, string right)
		{
			if ((Left = DbQueryOperand.Parse(table, left)) == null ||
				(Right = DbQueryOperand.Parse(table, right)) == null)
			{
				throw new ArgumentException($"Database query expression invalid");
			}
			//test types

			Operator = new DbQueryOperator(oper);
		}

		public bool Evaluate(ref object[] values)
		{
			var l = Left.Value(ref values);
			var r = Right.Value(ref values);


			return false;
		}

		bool Comparer<T>(T left, DbQueryConditionOper comp, T right)
			where T : IComparable<T>
		{
			var result = left.CompareTo(right);
			switch (Operator.OperatorType)
			{
				case DbQueryConditionOper.Equal:
					return result == 0;
				case DbQueryConditionOper.NotEqual:
					return result != 0;
				case DbQueryConditionOper.Greater:
					return result > 0;
				case DbQueryConditionOper.GreaterOrEqual:
					return result >= 0;
				case DbQueryConditionOper.Less:
					return result < 0;
				case DbQueryConditionOper.LessOrEqual:
					return result <= 0;
			}
			return false;
		}

		public override string ToString() => $"{Left} {Operator} {Right}";
	}

	#endregion

	#region Logical Operands supported AND, OR

	public enum DbQueryLogic
	{
		Undefined,
		AND,
		OR
	}

	public class DbQueryLogical : DbQueryExpressionBase
	{
		public override bool IsExpression => false;

		public DbQueryLogic LogicalType { get; protected internal set; }

		public string Name { get; protected internal set; }

		protected internal DbQueryLogical(string condition)
		{
			if (String.IsNullOrWhiteSpace(condition))
			{
				throw new ArgumentException($"Invalid condition, cannot be null or empty");
			}
			switch (Name = condition.ToUpper())
			{
				case "AND":
					LogicalType = DbQueryLogic.AND;
					break;
				case "OR":
					LogicalType = DbQueryLogic.OR;
					break;
				default:
					throw new ArgumentException($"Invalid condition [{Name}]");
			}
		}

		public override string ToString() => $"{Name}";
	}

	#endregion

}
