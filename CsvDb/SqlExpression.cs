using System;

namespace CsvDb
{
	public partial class DbQuery
	{

		/// <summary>
		/// SQL Query WHERE expression enumerate
		/// </summary>
		public enum ExpressionEnum
		{
			Expression,
			Logical
		}

		/// <summary>
		/// SQL Query WHERE abstract expression class
		/// </summary>
		public abstract class ExpressionBase
		{
			/// <summary>
			/// gets the expression type
			/// </summary>
			public ExpressionEnum Type { get; }

			/// <summary>
			/// creates an expression base
			/// </summary>
			/// <param name="type">expression type</param>
			internal ExpressionBase(ExpressionEnum type)
			{
				Type = type;
			}
		}

		/// <summary>
		/// SQL Query WHERE expression class
		/// </summary>
		public class Expression : ExpressionBase
		{
			/// <summary>
			/// Left operand
			/// </summary>
			public Operand Left { get; }

			/// <summary>
			/// Expression operator
			/// </summary>
			public Operator Operator { get; }

			/// <summary>
			/// Right operand
			/// </summary>
			public Operand Right { get; }

			/// <summary>
			/// gets the database table column in the expression
			/// </summary>
			public Operand Column
			{
				get
				{
					return Left.GroupType == OperandType.Column ?
						Left :
						Right;
				}
			}

			/// <summary>
			/// returns true if left and right operands are table columns
			/// </summary>
			public bool TwoColumns => Left.GroupType == OperandType.Column && Right.GroupType == OperandType.Column;

			/// <summary>
			/// creates a WHERE expression
			/// </summary>
			/// <param name="left">left expression</param>
			/// <param name="oper">operator</param>
			/// <param name="right">right expression</param>
			internal Expression(Operand left, TokenType oper, Operand right)
				: base(ExpressionEnum.Expression)
			{
				if ((Left = left) == null)
				{
					throw new ArgumentException($"invalid left operand in expression: {left}");
				}
				if ((Right = right) == null)
				{
					throw new ArgumentException($"invalid right operand in expression: {right}");
				}
				Operator = new Operator(oper);
			}

			public override string ToString() => $"{Left} {Operator} {Right}";
		}

		/// <summary>
		/// AND, OR logical expression
		/// </summary>
		public class LogicalExpression : ExpressionBase
		{
			/// <summary>
			/// logical type
			/// </summary>
			public TokenType Logical { get; }

			/// <summary>
			/// creates a logical expression AND, OR
			/// </summary>
			/// <param name="logical"></param>
			internal LogicalExpression(TokenType logical)
				: base(ExpressionEnum.Logical)
			{
				if (!((Logical = logical) == TokenType.AND || Logical == TokenType.OR))
				{
					throw new ArgumentException($"Invalid logical operator: {logical}");
				}
			}

			public override string ToString() => $"{Logical}";

		}

		/// <summary>
		/// SQL Query WHERE expression operator class
		/// </summary>
		public class Operator
		{
			/// <summary>
			/// operator type
			/// </summary>
			public TokenType Token { get; }

			/// <summary>
			/// creates an expression operator
			/// </summary>
			/// <param name="oper">operator type</param>
			public Operator(TokenType oper)
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

			public override string ToString()
			{
				switch (Token)
				{
					case TokenType.Equal:
						return "=";
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
		/// SQL Query WHERE abstract expression operand class
		/// </summary>
		public abstract class Operand
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
			/// Man group type of operand
			/// </summary>
			public OperandType GroupType { get; }

			/// <summary>
			/// Atomic type of operand
			/// </summary>
			public abstract DbColumnType Type { get; }

			/// <summary>
			/// true if operand is a database table column
			/// </summary>
			public abstract bool IsColumn { get; }

			/// <summary>
			/// gets the value of the operand
			/// </summary>
			/// <returns></returns>
			public abstract object Value();

			/// <summary>
			/// creates an expression operand
			/// </summary>
			/// <param name="groupType">main group type</param>
			/// <param name="text">text</param>
			/// <param name="cast">cast, None if doesnt apply</param>
			internal Operand(OperandType groupType, string text, DbColumnType cast = DbColumnType.None)
			{
				GroupType = groupType;
				Cast = cast;
				Text = text;
			}

			public override string ToString() => $"{(Cast != DbColumnType.None ? $"({Cast})" : "")}{Text}";

		}

		/// <summary>
		/// represents a expression constant operand
		/// </summary>
		public abstract class ConstantOperand : Operand
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
			internal ConstantOperand(OperandType groupType, string text, DbColumnType cast = DbColumnType.None)
				: base(groupType, text, cast)
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
			public StringOperand(string text, DbColumnType cast = DbColumnType.None)
				: base(OperandType.String, text.UnwrapQuotes(), cast)
			{ }

			public override object Value() => Text;
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
			public NumberOperand(string text, DbColumnType cast = DbColumnType.None)
				: base(OperandType.Number, text, cast)
			{
				if ((valueType = text.ToNumberType()) == null)
				{
					throw new ArgumentException($"Invalid number operand: {text}");
				}
			}

			public override object Value() => valueType.Item2;
		}

		/// <summary>
		/// SQL Query WHERE abstract expression column operand class
		/// </summary>
		public class ColumnOperand : Operand
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
				: base(OperandType.Column, column == null ? String.Empty : column.Identifier(), cast)
			{
				if ((Column = column) == null)
				{
					throw new ArgumentException($"Column operand null or empty");
				}
				Type = Enum.Parse<DbColumnType>(Column.Meta.Type);
			}

			/// <summary>
			/// throws an exception, can't evaluate
			/// </summary>
			/// <returns></returns>
			public override object Value()
			{
				throw new NotImplementedException("Cannot get value of table column operand");
			}

		}

	}

}
