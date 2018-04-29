using System;
using System.Collections.Generic;
using System.Text;

namespace CsvDb
{
	public partial class DbQuery
	{
		//[INNER|CROSS|(LEFT|RIGHT|FULL) OUTER] JOIN table0 t0 ON expr:<left> oper <right> 
		/// <summary>
		/// SQL Query JOIN clause
		/// </summary>
		public sealed class SqlJoin
		{
			public TokenType Token { get; }

			public Table Table { get; }

			public Expression Expression { get; }

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
