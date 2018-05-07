using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace CsvDb
{
	public partial class DbQuery
	{

		public enum ExpressionItemType
		{
			Operand,
			Operator,
			Casting
		}

		public interface IValuesOf
		{
			object[] CurrentRow(string table);
		}

		public abstract class ExpressionItem
		{
			public ExpressionItemType ExpressionType { get; }

			/// <summary>
			/// this's used when there is no indexed column, so we have to get them from table
			/// </summary>
			/// <param name="valuesOf">function to get current row values</param>
			/// <returns></returns>
			public abstract ExpressionValue Evaluate(IValuesOf valuesOf);

			internal ExpressionItem(ExpressionItemType type)
			{
				ExpressionType = type;
			}

			public override string ToString() => ExpressionType.ToString();
		}

		public class ExpressionValue
		{
			public DbColumnType Type { get; }

			public object Value { get; }

			public T ValueAs<T>()
			{
				return (T)Convert.ChangeType(Value, Type.TypeCode());
			}

			public bool CompareTo(TokenType oper, ExpressionValue other)
			{
				if (other == null)
				{
					throw new ArgumentException($"cannot compare: {Type} to null");
				}
				var commonType = Type.Normalize(other.Type);
				if (commonType == DbColumnType.None)
				{
					throw new ArgumentException($"type: {Type} cannot compare to: {other.Type}");
				}
				switch (oper)
				{
					case TokenType.AND:
						return ValueAs<bool>() && other.ValueAs<bool>();
					case TokenType.OR:
						return ValueAs<bool>() || other.ValueAs<bool>();
					case TokenType.Equal:
					case TokenType.NotEqual:
					case TokenType.Greater:
					case TokenType.GreaterOrEqual:
					case TokenType.Less:
					case TokenType.LessOrEqual:
						//generic method
						MethodInfo method =
							this.GetType().GetMethod(nameof(CompareTo), BindingFlags.Instance | BindingFlags.NonPublic);
						//make generic
						MethodInfo genMethod = method.MakeGenericMethod(System.Type.GetType($"System.{commonType}"));
						//invoke
						var result = (bool)genMethod.Invoke(this, new object[] { oper, other });
						//
						return result;
				}
				throw new NotImplementedException($"operator: {oper} not implemented yet");
			}

			/// <summary>
			/// Compare two values
			/// </summary>
			/// <typeparam name="T"></typeparam>
			/// <param name="oper"></param>
			/// <param name="other"></param>
			/// <returns></returns>
			internal bool CompareTo<T>(TokenType oper, ExpressionValue other)
				where T : IComparable<T>
			{
				var thisValue = ValueAs<T>();
				var otherValue = other.ValueAs<T>();
				switch (oper)
				{
					case TokenType.Equal:
						return thisValue.CompareTo(otherValue) == 0;
					case TokenType.NotEqual:
						return thisValue.CompareTo(otherValue) != 0;
					case TokenType.Greater:
						return thisValue.CompareTo(otherValue) > 0;
					case TokenType.GreaterOrEqual:
						return thisValue.CompareTo(otherValue) >= 0;
					case TokenType.Less:
						return thisValue.CompareTo(otherValue) < 0;
					case TokenType.LessOrEqual:
						return thisValue.CompareTo(otherValue) <= 0;
					default:
						return false;
				}
			}

			public ExpressionValue(DbColumnType type, object value)
			{
				Type = type;
				Value = value;
			}

			public override string ToString() => $"({Type}){Value}";
		}

		public interface IBinaryTree<T>
		{
			T Left { get; }

			IBinaryTree<T> Data { get; }

			T Right { get; }
		}

		#region Operators

		/// <summary>
		/// SQL Query WHERE expression class
		/// </summary>
		public abstract class ExpressionOperator : ExpressionItem//, IBinaryTree<ExpressionItem>
		{
			/// <summary>
			/// Left operand
			/// </summary>
			public ExpressionItem Left { get; }

			/// <summary>
			/// Expression operator
			/// </summary>
			public TokenType Operator { get; }

			public abstract String OperatorText { get; }

			/// <summary>
			/// Right operand
			/// </summary>
			public ExpressionItem Right { get; }

			//public IBinaryTree<ExpressionItem> Data => this;

			/// <summary>
			/// returns true if left and right operands are table columns
			/// </summary>
			//public bool TwoColumns => Left.GroupType == OperandType.Column && Right.GroupType == OperandType.Column;

			/// <summary>
			/// creates a WHERE expression
			/// </summary>
			/// <param name="left">left expression</param>
			/// <param name="oper">operator</param>
			/// <param name="right">right expression</param>
			internal ExpressionOperator(ExpressionItem left, TokenType oper, ExpressionItem right)
				: base(ExpressionItemType.Operator)
			{
				if ((Left = left) == null)
				{
					throw new ArgumentException($"invalid left operand in expression: {left}");
				}
				if ((Right = right) == null)
				{
					throw new ArgumentException($"invalid right operand in expression: {right}");
				}
				Operator = oper;
			}

			public override string ToString() => $"{Left} {OperatorText} {Right}";

			internal static ExpressionOperator Create(ExpressionItem left, TokenType oper, ExpressionItem right)
			{
				if (oper == TokenType.AND || oper == TokenType.OR)
				{
					return new LogicalOperator(left, oper, right);
				}
				else if (oper.IsComparison())
				{
					return new ComparisonOperator(left, oper, right);
				}
				return null;
			}

			public override ExpressionValue Evaluate(IValuesOf valuesOf)
			{
				var left = Left.Evaluate(valuesOf);

				var right = Right.Evaluate(valuesOf);

				return new ExpressionValue(DbColumnType.Bool, left.CompareTo(Operator, right));
			}
		}


		/// <summary>
		/// AND, OR logical expression
		/// </summary>
		public class LogicalOperator : ExpressionOperator
		{

			public override string OperatorText => Operator.ToString();

			/// <summary>
			/// creates a logical expression AND, OR
			/// </summary>
			/// <param name="logical"></param>
			internal LogicalOperator(ExpressionItem left, TokenType logical, ExpressionItem right)
				: base(left, logical, right)
			{
				if (!(logical == TokenType.AND || logical == TokenType.OR))
				{
					throw new ArgumentException($"Invalid logical operator: {logical}");
				}
			}

		}


		/// <summary>
		/// SQL Query WHERE expression operator class
		/// </summary>
		public class ComparisonOperator : ExpressionOperator
		{
			/// <summary>
			/// operator type
			/// </summary>
			public TokenType Token { get; }

			public override string OperatorText
			{
				get
				{
					switch (Token)
					{
						case TokenType.Assign:
							return "=";
						case TokenType.Equal:
							return "==";
						case TokenType.NotEqual:
							return "<>";
						case TokenType.Greater:
							return ">";
						case TokenType.GreaterOrEqual:
							return ">=";
						case TokenType.Less:
							return "<";
						case TokenType.LessOrEqual:
							return "<=";
						default:
							return null;
					}
				}
			}

			/// <summary>
			/// creates an expression operator
			/// </summary>
			/// <param name="oper">operator type</param>
			public ComparisonOperator(ExpressionItem left, TokenType oper, ExpressionItem right)
				: base(left, oper, right)
			{
				switch (oper)
				{
					case TokenType.Equal:
					case TokenType.NotEqual:
					case TokenType.Greater:
					case TokenType.GreaterOrEqual:
					case TokenType.Less:
					case TokenType.LessOrEqual:
						Token = oper;
						break;
					default:
						throw new ArgumentException($"Invalid sql comparison operator: {oper}");
				}
			}

		}

		#endregion

		#region Operands

		/// <summary>
		/// SQL Query WHERE abstract expression operand class
		/// </summary>
		public abstract class ExpressionOperand : ExpressionItem
		{
			//column name or Identifier, [table identifier].column name, 
			//constant: String, Number

			/// <summary>
			/// operand cast if any (cast type)operand
			/// </summary>
			public DbColumnType Cast { get; }

			/// <summary>
			/// operand text
			/// </summary>
			public string Text { get; protected internal set; }

			/// <summary>
			/// Atomic type of operand
			/// </summary>
			public abstract DbColumnType Type { get; }

			/// <summary>
			/// true if operand is a database table column
			/// </summary>
			public abstract bool IsColumn { get; }

			/// <summary>
			/// creates an expression operand
			/// </summary>
			/// <param name="text">text</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			internal ExpressionOperand(string text, DbColumnType cast = DbColumnType.None)
				: base(ExpressionItemType.Operand)
			{
				Cast = cast;
				Text = text;
			}

			public override string ToString() => $"{(Cast != DbColumnType.None ? $"({Cast})" : "")}{Text}";

		}

		/// <summary>
		/// represents a expression constant operand
		/// </summary>
		public abstract class ConstantOperand : ExpressionOperand
		{
			/// <summary>
			/// returns false
			/// </summary>
			public override bool IsColumn => false;

			/// <summary>
			/// creates an expression constant operand
			/// </summary>
			/// <param name="groupType">main group type</param>
			/// <param name="text">text</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			internal ConstantOperand(string text, DbColumnType cast)
				: base(text, cast)
			{ }
		}

		/// <summary>
		/// represents a expression string operand
		/// </summary>
		public class StringOperand : ConstantOperand
		{
			/// <summary>
			/// an String constant operand
			/// </summary>
			public override DbColumnType Type => DbColumnType.String;

			/// <summary>
			/// creates an expression string oeprand
			/// </summary>
			/// <param name="text">text</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			public StringOperand(string text)
				: base(text.UnwrapQuotes(), DbColumnType.None)
			{ }

			public override ExpressionValue Evaluate(IValuesOf valuesOf)
			{
				return new ExpressionValue(DbColumnType.String, Text);
			}

			public override string ToString() => $"{(Cast != DbColumnType.None ? $"({Cast})" : "")}'{Text}'";

		}

		/// <summary>
		/// represents a expression numeric operand
		/// </summary>
		public class NumberOperand : ConstantOperand
		{
			Tuple<DbColumnType, object> valueType = null;

			/// <summary>
			/// returns the type of the number
			/// </summary>
			public override DbColumnType Type => valueType.Item1;

			/// <summary>
			/// creates an expression numeric operand
			/// </summary>
			/// <param name="text">text</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			public NumberOperand(string text, DbColumnType cast)
				: base(text, cast)
			{
				if ((valueType = text.ToNumberType()) == null)
				{
					throw new ArgumentException($"Invalid number operand: {text}");
				}
				if (cast != DbColumnType.None && !cast.IsNumeric())
				{
					throw new ArgumentException($"Invalid numeric cast: {cast}");
				}
			}

			public override ExpressionValue Evaluate(IValuesOf valuesOf)
			{
				return new ExpressionValue(Type, valueType.Item2);
			}

		}

		/// <summary>
		/// SQL Query WHERE abstract expression column operand class
		/// </summary>
		public class ColumnOperand : ExpressionOperand
		{
			/// <summary>
			/// gets the column operand
			/// </summary>
			public Column Column { get; }

			/// <summary>
			/// returns true
			/// </summary>
			public override bool IsColumn => true;

			/// <summary>
			/// returns the table column type
			/// </summary>
			public override DbColumnType Type { get; }

			/// <summary>
			/// creates an expression column operand
			/// </summary>
			/// <param name="column">column</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			public ColumnOperand(Column column, DbColumnType cast = DbColumnType.None)
				: base(column == null ? String.Empty : column.Identifier(), cast)
			{
				if ((Column = column) == null)
				{
					throw new ArgumentException($"Column operand null or empty");
				}
				Type = Enum.Parse<DbColumnType>(Column.Meta.Type);
				if (cast != DbColumnType.None)
				{
					if ((Type.IsNumeric() && !cast.IsNumeric()))
					{
						throw new ArgumentException($"cast types: {Type} and {cast} doesnot match");
					}
				}
			}

			public override ExpressionValue Evaluate(IValuesOf valuesOf)
			{
				var columnValues = valuesOf.CurrentRow(Column.Meta.TableName);

				return new ExpressionValue(Type, columnValues[Column.Meta.Index]);
			}
		}

		#endregion

		public class CastingExpression : ExpressionItem
		{
			public DbColumnType Type { get; }

			public CastingExpression(DbColumnType type)
				: base(ExpressionItemType.Casting)
			{
				Type = type;
			}

			public override ExpressionValue Evaluate(IValuesOf valuesOf)
			{
				//if you reach here, there's a big bug 
				throw new NotImplementedException();
			}

			public override string ToString() => $"({Type})";
		}

	}

}
