using System;
using System.Collections.Generic;
using System.Text;

namespace CsvDb
{
	public partial class DbQuery
	{
		/// <summary>
		/// SQL Query JOIN clause. [INNER|CROSS|(LEFT|RIGHT|FULL) OUTER] JOIN table0 t0 ON expr:<left> oper <right>
		/// </summary>
		public sealed class SqlJoin
		{
			/// <summary>
			/// sql join type
			/// </summary>
			public TokenType Token { get; }

			/// <summary>
			/// table used in the join
			/// </summary>
			public Table Table { get; }

			/// <summary>
			/// join expression
			/// </summary>
			public Expression Expression { get; }

			/// <summary>
			/// creates an SQL query JOIN
			/// </summary>
			/// <param name="token">join type</param>
			/// <param name="table">join table</param>
			/// <param name="expression">join expression</param>
			internal SqlJoin(TokenType token, Table table, Expression expression)
			{
				if (!JoinStarts.Contains(Token = token) || (Table = table) == null || (Expression = expression) == null)
				{
					throw new ArgumentException("Invalid SQL JOIN");
				}
			}

			public override string ToString()
			{
				var sb = new StringBuilder();
				sb.Append(Token.ToString());
				if (Token == TokenType.LEFT || Token == TokenType.RIGHT || Token == TokenType.FULL)
				{
					sb.Append(" OUTER");
				}
				sb.Append($" JOIN {Table} ON {Expression}");

				return sb.ToString();
			}
		}

	}
}
