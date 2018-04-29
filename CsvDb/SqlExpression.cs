using System;
using System.Collections.Generic;
using System.Text;

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
			public ExpressionEnum Type { get; }

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

			public Operand Column
			{
				get
				{
					return Left.GroupType == OperandEnum.Column ?
						Left :
						Right;
				}
			}

			public bool TwoColumns => Left.GroupType == OperandEnum.Column && Right.GroupType == OperandEnum.Column;

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
		/// AND, OR
		/// </summary>
		public class LogicalExpression : ExpressionBase
		{
			public TokenType Logical { get; }

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
			public TokenType Token { get; }

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

			public DbColumnTypeEnum Cast { get; }

			public string Text { get; protected internal set; }

			/// <summary>
			/// Man group type of operand
			/// </summary>
			public OperandEnum GroupType { get; }

			/// <summary>
			/// Atomic type of operand
			/// </summary>
			public abstract DbColumnTypeEnum Type { get; }

			public abstract bool IsColumn { get; }

			public abstract object Value();

			internal Operand(OperandEnum groupType, string text, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
			{
				GroupType = groupType;
				Cast = cast;
				Text = text;
			}

			public override string ToString() => $"{(Cast != DbColumnTypeEnum.None ? $"({Cast})" : "")}{Text}";

		}

		public abstract class ConstantOperand : Operand
		{
			public override bool IsColumn => false;

			internal ConstantOperand(OperandEnum groupType, string text, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
				: base(groupType, text, cast)
			{ }
		}

		public class StringOperand : ConstantOperand
		{
			public override DbColumnTypeEnum Type => DbColumnTypeEnum.String;

			public StringOperand(string text, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
				: base(OperandEnum.String, text.UnwrapQuotes(), cast)
			{ }

			public override object Value() => Text;
		}

		public class NumberOperand : ConstantOperand
		{
			Tuple<DbColumnTypeEnum, object> valueType = null;

			public override DbColumnTypeEnum Type => valueType.Item1;

			public NumberOperand(string text, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
				: base(OperandEnum.Number, text, cast)
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
			public Column Column { get; }

			public override bool IsColumn => true;

			public override  DbColumnTypeEnum Type { get; }

			public ColumnOperand(Column column, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
				: base(OperandEnum.Column, column == null ? String.Empty : column.Identifier(), cast)
			{
				if ((Column = column) == null)
				{
					throw new ArgumentException($"Column operand null or empty");
				}
				Type = Enum.Parse<DbColumnTypeEnum>(Column.Meta.Type);
			}

			public override object Value()
			{
				throw new NotImplementedException("Cannot get value of table column operand");
			}

			//public ColumnOperand(string column, string identifier = null, DbColumnTypeEnum cast = DbColumnTypeEnum.None)
			//	: base(OperandEnum.Column,
			//			(String.IsNullOrWhiteSpace(identifier)) ? column : $"{identifier}.{column}",
			//			cast)
			//{
			//	ColumnName = column;
			//	ColumnAlias = identifier;
			//}
		}

	}

}
