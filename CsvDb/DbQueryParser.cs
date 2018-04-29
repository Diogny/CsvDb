using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb
{
	public partial class DbQuery
	{
		/// <summary>
		/// JOIN start keywords
		/// </summary>
		internal static List<TokenType> JoinStarts = new List<TokenType>()
		{
			TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL, TokenType.CROSS
		};

		/// <summary>
		/// SELECT functions
		/// </summary>
		internal static List<TokenType> SelectFunctions = new List<TokenType>()
		{
			TokenType.COUNT, TokenType.AVG, TokenType.SUM,
			TokenType.MIN, TokenType.MAX
		};

		/// <summary>
		/// known sql keywords for the scope of this app
		/// </summary>
		internal static List<string> KeyWords;

		/// <summary>
		/// collection of casting types
		/// </summary>
		internal static List<TokenType> CastingTypes = new List<TokenType> {
			//numeric
			TokenType.Byte, TokenType.Int16, TokenType.Int32, TokenType.Int64, TokenType.Float,
			TokenType.Double, TokenType.Decimal,
			//char/string
			TokenType.Char, TokenType.String
		};

		/// <summary>
		/// collection of expression operators
		/// </summary>
		internal static List<TokenType> OperatorList = new List<TokenType>()
		{
			TokenType.Equal, TokenType.NotEqual,
			TokenType.Greater, TokenType.GreaterOrEqual,
			TokenType.Less, TokenType.LessOrEqual
		};

		/// <summary>
		/// parse sql query text into tokens
		/// </summary>
		/// <param name="query">sql query text</param>
		/// <returns></returns>
		internal static IEnumerable<TokenItem> ParseTokens(string query)
		{
			if (String.IsNullOrWhiteSpace(query))
			{
				throw new ArgumentException("Invalid query");
			}

			int index = 0;
			int startOfToken;
			char currentChar = (char)0;
			startOfToken = -1;

			char[] queryBuffer = query.ToCharArray();
			int length = queryBuffer.Length;
			var charString = new StringBuilder();

			bool CanRead() => index < length;

			char PeekChar() { return ReadChar(peek: true, append: false); }

			TokenItem AddToken(TokenType token, String text = null, bool readNext = false)
			{
				if (readNext)
				{
					ReadChar();
				}
				if (text == null)
				{
					text = charString.ToString();
				}
				var tk = new TokenItem(token, text, startOfToken);
				//list.Add(tk);
				charString.Length = 0;
				startOfToken = -1;
				//
				return tk;
			}

			char ReadChar(bool recordStart = false, bool skipCanRead = false, bool peek = false, bool append = true)
			{
				if (skipCanRead || CanRead())
				{
					if (recordStart)
					{
						startOfToken = index;
					}
					currentChar = queryBuffer[index];
					if (!peek)
					{
						index++;
					}
					if (append)
					{
						charString.Append(currentChar);
					}

					return currentChar;
				}
				return (char)0;
				//throw new ArgumentException("Buffer overflow");
			}

			while (CanRead())
			{
				switch (ReadChar(recordStart: true))
				{
					case ',':
						yield return AddToken(TokenType.Comma);
						break;
					case ';':
						yield return AddToken(TokenType.SemiColon);
						break;
					case '*':
						yield return AddToken(TokenType.Astherisk);
						break;
					case '(':
						yield return AddToken(TokenType.OpenPar);
						break;
					case ')':
						yield return AddToken(TokenType.ClosePar);
						break;
					case '=':
						yield return AddToken(TokenType.Equal);
						break;
					case '<':
						switch (PeekChar())
						{
							case '=':  // <=
								yield return AddToken(TokenType.LessOrEqual, text: null, readNext: true);
								break;
							case '>':   // <>
								yield return AddToken(TokenType.NotEqual, text: null, readNext: true);
								break;
							default:   // <
								yield return AddToken(TokenType.Less);
								break;
						}
						break;
					case '>':
						if (PeekChar() == '=')
						{
							yield return AddToken(TokenType.GreaterOrEqual, text: null, readNext: true);   // >=
						}
						else
						{
							yield return AddToken(TokenType.Greater);   // >
						}
						break;
					case '\'':
						//string
						bool endOfString = false;
						bool endOfStream = false;

						while (!(endOfStream = !CanRead()) && !endOfString)
						{
							//read as many as possible
							while (!(endOfStream = !CanRead()) && ReadChar(skipCanRead: true, append: false) != '\'')
							{
								charString.Append(currentChar);
							}
							if (endOfStream)
							{
								throw new ArgumentException($"Expected string terminator on pos:{index - 1}");
							}
							//we got a " store it
							charString.Append(currentChar);

							if (PeekChar() == '\'')
							{
								//store "" and continue
								ReadChar();
								//charString.Append($"{currentChar}{ReadChar(append: false)}");
							}
							else
							{
								//end of string reached, test for last "
								if (charString[charString.Length - 1] != '\'')
								{
									throw new ArgumentException($"End of stream reached while reading string identifier");
								}
								endOfString = true;
								//charString.Append("\"");
							}
						}
						yield return AddToken(TokenType.String);
						break;
					case '.':
						yield return AddToken(TokenType.Dot);
						break;
					default:
						if (char.IsDigit(currentChar))
						{
							// later allow -0.45  .98
							//number
							//charString.Append(currentChar);
							while (char.IsDigit(PeekChar()))
							{
								charString.Append(ReadChar(append: false));
							}
							//
							if (PeekChar() == '.')
							{
								charString.Append(ReadChar(append: false));
								while (char.IsDigit(PeekChar()))
								{
									charString.Append(ReadChar(append: false));
								}
							}
							yield return AddToken(TokenType.Number);
						}
						else if ((int)currentChar <= 32)
						{
							//discard spaces
							charString.Length = 0;
						}
						else
						{
							//starts with Letter or _
							bool isIdentifier = char.IsLetter(currentChar) || currentChar == '_';
							if (isIdentifier)
							{
								//identifier
								while (isIdentifier)
								{
									//charString.Append(currentChar);
									var peek = PeekChar();
									//continue if Letter, Number or _
									if ((isIdentifier = char.IsLetterOrDigit(peek) || peek == '_'))
									{
										ReadChar();
									}
								}
								//
								var str = charString.ToString();

								var ndx = KeyWords.IndexOf(str);
								if (ndx >= 0)
								{
									//key word
									yield return AddToken((TokenType)(ndx + 1), text: str);
								}
								else
								{
									//an identifier
									yield return AddToken(TokenType.Identifier, text: str);
								}
								//if (Enum.TryParse<Token>(str, out Token result))
							}
							else
							{
								throw new ArgumentException($"Unexpected char: {currentChar} at position: {index - 1}");
							}
						}
						break;
				}
			}
		}

		/// <summary>
		/// static initialization
		/// </summary>
		static DbQuery()
		{
			KeyWords =
				Enumerable
					.Range(1, (int)Consts.LastKeywordToken)
					.Select(i => ((TokenType)i).ToString()).ToList();
		}

		/// <summary>
		/// parse an sql query text
		/// </summary>
		/// <param name="query">sql query text</param>
		/// <param name="validator">sql query validator against a database</param>
		/// <returns></returns>
		public static DbQuery Parse(string query, IQueryValidation validator)
		{
			if (validator == null)
			{
				throw new ArgumentException("No query database validator provided");
			}

			int top = -1;
			ColumnsSelect columnSelect = null;
			var tableCollection = new List<Table>();
			SqlJoin join = null;
			var where = new List<ExpressionBase>();
			int limitValue = -1;

			TokenItem PreviousToken = default(TokenItem);
			TokenItem CurrentToken = default(TokenItem);
			var peekedToken = default(TokenItem);
			var columnCollection = new List<Column>();

			var queue = new Queue<TokenItem>(ParseTokens(query));

#if DEBUG
			Console.WriteLine("Query tokens:");
			Console.WriteLine($"  {String.Join($"{Environment.NewLine}  ", queue)}{Environment.NewLine}");
#endif

			bool EndOfStream = queue.Count == 0;

			bool GetToken()
			{
				if (EndOfStream || (EndOfStream = !queue.TryDequeue(out TokenItem tk)))
				{
					return false;
				}
				//save previous
				PreviousToken = CurrentToken;
				//get current
				CurrentToken = tk;

				return true;
			}

			bool PeekToken()
			{
				if (!EndOfStream && queue.TryPeek(out peekedToken))
				{
					return true;
				}
				return false;
			}

			bool GetTokenIf(TokenType token)
			{
				if (!EndOfStream && queue.TryPeek(out peekedToken) && peekedToken.Token == token)
				{
					CurrentToken = queue.Dequeue();
					return true;
				}
				return false;
			};

			bool GetTokenIfContains(List<TokenType> tokens)
			{
				if (!EndOfStream && queue.TryPeek(out peekedToken) && tokens.Contains(peekedToken.Token))
				{
					CurrentToken = queue.Dequeue();
					return true;
				}
				return false;
			}

			Column ReadColumnName()
			{
				Column selectColumn = null;
				//read operand [(i).](column)
				if (GetTokenIf(TokenType.Identifier))
				{
					var tableAlias = CurrentToken.Value;
					String columnName = null;
					String columnAlias = null;

					if (GetTokenIf(TokenType.Dot))
					{
						if (!GetToken())
						{
							throw new ArgumentException($"column name expected");
						}
						columnName = CurrentToken.Value;
					}
					else
					{
						columnName = tableAlias;
						tableAlias = null;
					}
					//alias
					if (GetTokenIf(TokenType.AS))
					{
						if (!GetTokenIf(TokenType.Identifier))
						{
							throw new ArgumentException($"Column alias name expected after AS: {CurrentToken.Value}");
						}
						columnAlias = CurrentToken.Value;
					}
					selectColumn = new Column(columnName, tableAlias, columnAlias);
				}
				else
				{
					throw new ArgumentException($"quantifier column expected");
				}
				return selectColumn;
			}

			Operand ReadOperand(bool readCasting = true)
			{
				//can be a column, string, number
				if (!GetToken())
				{
					throw new ArgumentException("operand expected");
				}

				DbColumnType casting = DbColumnType.None;
				//try to read cast
				if (readCasting && (CurrentToken.Token == TokenType.OpenPar))
				{
					if (!GetTokenIfContains(CastingTypes) ||
						(casting = CurrentToken.Token.ToCast()) == DbColumnType.None)
					{
						throw new ArgumentException($"invalid casting type: {CurrentToken.Value}");
					}
					if (!GetTokenIf(TokenType.ClosePar))
					{
						throw new ArgumentException("close parenthesis expected after casting type");
					}
					//read next
					if (!GetToken())
					{
						throw new ArgumentException($"operand expected after casting ({casting})");
					}
				}

				//read operand
				switch (CurrentToken.Token)
				{
					case TokenType.Identifier:
						var columnName = CurrentToken.Value;
						String columnIdentifier = null;

						if (GetTokenIf(TokenType.Dot))
						{
							if (!GetToken())
							{
								throw new ArgumentException($"column name expected");
							}
							columnIdentifier = columnName;
							columnName = CurrentToken.Value;
						}

						Column col = null;
						//try to find column in known tables
						if (columnIdentifier != null)
						{
							//find in table by its alias
							var aliasTable = tableCollection.FirstOrDefault(t => t.Alias == columnIdentifier);
							if (aliasTable == null)
							{
								throw new ArgumentException($"cannot find column: {columnName}");
							}
							col = new Column(columnName, aliasTable.Name);
						}
						else
						{
							//find the only first in all known tables
							var tables = tableCollection.Where(t => validator.TableHasColumn(t.Name, columnName)).ToList();
							if (tables.Count != 1)
							{
								throw new ArgumentException($"column: {columnName} could not be found in database or cannot resolve multiple tables");
							}
							//tableName = tables[0].Name;
							col = new Column(columnName, validator.ColumnMetadata(tables[0].Name, columnName));
						}
						return new ColumnOperand(col, casting);
					case TokenType.String:
						return new StringOperand(CurrentToken.Value, casting);
					case TokenType.Number:
						return new NumberOperand(CurrentToken.Value, casting);
					default:
						throw new ArgumentException("Operand expected");
				}
			}

			#region SELECT

			if (!GetTokenIf(TokenType.SELECT))
			{
				throw new ArgumentException("SELECT expected");
			}

			//see if it's a function
			if (GetTokenIfContains(SelectFunctions))
			{
				var function = CurrentToken.Token;

				if (!GetTokenIf(TokenType.OpenPar))
				{
					throw new ArgumentException($"expected ( after FUNCTION {CurrentToken.Token}");
				}

				// COUNT([(i).](column))
				var functionColumn = ReadColumnName();

				if (!GetTokenIf(TokenType.ClosePar))
				{
					throw new ArgumentException($"expected ) closing {CurrentToken.Token}");
				}

				//alias
				String functionColumnAlias = null;
				if (GetTokenIf(TokenType.AS))
				{
					if (!GetTokenIf(TokenType.Identifier))
					{
						throw new ArgumentException($"");
					}
					functionColumnAlias = CurrentToken.Value;
				}
				columnSelect = new ColumnsSelect(function, functionColumn, functionColumnAlias);
			}
			else
			{
				//preceded by TOP if any
				//TOP integer PERCENT
				if (GetTokenIf(TokenType.TOP))
				{
					if (!GetTokenIf(TokenType.Number))
					{
						throw new ArgumentException($"Number expected after TOP");
					}
					//test for integer
					if (!int.TryParse(CurrentToken.Value, out top) || top <= 0)
					{
						throw new ArgumentException($"TOP [positive integer greater than 0] expected: {CurrentToken.Value}");
					}
					//PERCENT
					if (GetTokenIf(TokenType.PERCENT))
					{
						//save if for later
					}
				}

				//read columns
				//read column selector: comma separated or *
				if (GetTokenIf(TokenType.Astherisk))
				{
					columnSelect = new ColumnsSelect(top);
				}
				else
				{
					//mut have at least one column identifier
					if (PeekToken() && peekedToken.Token != TokenType.Identifier)
					{
						throw new ArgumentException("table column name(s) expected");
					}

					//read first
					columnCollection.Add(ReadColumnName());

					while (GetTokenIf(TokenType.Comma))
					{
						//next
						columnCollection.Add(ReadColumnName());
					}
					columnSelect = new ColumnsSelect(top, columnCollection);
				}
			}

			#endregion

			#region FROM

			if (!GetTokenIf(TokenType.FROM))
			{
				throw new ArgumentException("FROM expected");
			}

			do
			{
				//read identifier: table name
				if (!GetTokenIf(TokenType.Identifier))
				{
					throw new ArgumentException("table name identifier after FROM expected");
				}

				var tableName = CurrentToken.Value;
				String tableAlias = null;

				//var pos = CurrentToken.Position;
				// [table name] AS [alias]
				if (GetTokenIf(TokenType.AS))
				{
					//create it so WHERE can handle it
					//tableIdentifier = new DbQueryTableIdentifier(table, CurrentToken.Value);
					if (!GetTokenIf(TokenType.Identifier))
					{
						throw new ArgumentException($"Table alias expected after AS");
					}
					tableAlias = CurrentToken.Value;
				}
				//tableIdentifier = new DbQueryTableIdentifier(table);
				var table = new Table(tableName, tableAlias);

				if (!validator.HasTable(table.Name))
				{
					throw new ArgumentException($"Invalid table name: {table.Name}");
				}

				tableCollection.Add(table);
			}
			while (GetTokenIf(TokenType.Comma));

			if (columnSelect.FullColumns)
			{
				//fill SELECT * with all columns in FROM if applies
				foreach (var col in tableCollection
					.SelectMany(
						table => validator.ColumnsOf(table.Name),
						(table, col) => new Column(col, validator.ColumnMetadata(table.Name, col))))
				{
					columnSelect.Add(col);
				}
			}
			else
			{
				//check all SELECT columns
				foreach (var col in columnSelect.AllColumns)
				{
					string tableName = null;

					if (col.HasTableAlias)
					{
						//find it in table collection
						var table = tableCollection.FirstOrDefault(t => t.HasAlias && col.TableAlias == t.Alias);
						if (table == null)
						{
							throw new ArgumentException($"column alias undefined {col}");
						}
						tableName = table.Name;
						if (!validator.TableHasColumn(tableName, col.Name))
						{
							throw new ArgumentException($"column: {col.Name} could not be found in table {tableName}");
						}
					}
					else
					{
						//brute force search
						var tables = tableCollection.Where(t => validator.TableHasColumn(t.Name, col.Name)).ToList();
						if (tables.Count != 1)
						{
							throw new ArgumentException($"column: {col.Name} could not be found in database or cannot resolve multiple tables");
						}
						tableName = tables[0].Name;
					}
					//link column to table, and find its index
					col.Meta = validator.ColumnMetadata(tableName, col.Name);
				}
			}

			#endregion

			#region JOIN

			if (GetTokenIfContains(JoinStarts))
			{
				//FROM table must has identifier, should be only one here

				TokenType JoinCommand = CurrentToken.Token;
				Expression JoinExpression = null;
				Table joinTable = null;

				if (CurrentToken.Token == TokenType.LEFT || CurrentToken.Token == TokenType.RIGHT ||
					CurrentToken.Token == TokenType.FULL)
				{
					//LEFT OUTER JOIN
					//RIGHT OUTER JOIN
					//FULL OUTER JOIN
					if (!GetTokenIf(TokenType.OUTER))
					{
						throw new ArgumentException($"OUTER expected after {CurrentToken.Token}");
					}
				}
				else if (!(CurrentToken.Token == TokenType.INNER || CurrentToken.Token == TokenType.CROSS))
				{
					//INNER JOIN
					//CROSS JOIN
					throw new ArgumentException($"invalid JOIN keyword {CurrentToken.Token}");
				}

				if (!GetTokenIf(TokenType.JOIN))
				{
					throw new ArgumentException($"JOIN expected after {CurrentToken.Token}");
				}

				//read table name
				if (!GetTokenIf(TokenType.Identifier))
				{
					throw new ArgumentException("table name identifier after FROM expected");
				}
				//check -table name
				var tableName = CurrentToken.Value;
				String tableAlias = null;

				// + identifier
				if (GetTokenIf(TokenType.AS))
				{
					if (!GetTokenIf(TokenType.Identifier))
					{
						throw new ArgumentException($"Table alias expected after AS");
					}
					tableAlias = CurrentToken.Value;
				}
				joinTable = new Table(tableName, tableAlias);

				if (!validator.HasTable(joinTable.Name))
				{
					throw new ArgumentException($"Invalid table name: {joinTable.Name}");
				}

				//read ON
				if (!GetTokenIf(TokenType.ON))
				{
					throw new ArgumentException($"ON expected in JOIN query");
				}

				//read expression
				//left & right operand must be from different tables
				//read left
				var left = ReadOperand(readCasting: false);

				//read operator
				if (!GetToken() || !CurrentToken.IsOperator)
				{
					throw new ArgumentException("operator of JOIN expression expected");
				}
				var oper = CurrentToken;

				//read right
				var right = ReadOperand(readCasting: false);

				//operands must be of type column with different table identifiers
				if (left.GroupType != OperandType.Column ||
					right.GroupType != OperandType.Column ||
					((ColumnOperand)left).Column.TableAlias == ((ColumnOperand)right).Column.TableAlias)
				{
					throw new ArgumentException("JOIN query expression table identifiers cannot be the same");
				}

				JoinExpression = new Expression(
					left,
					oper.Token,
					right
				);
				join = new SqlJoin(JoinCommand, joinTable, JoinExpression);

				//validate JOIN column tables




			}

			#endregion

			#region WHERE if any

			if (GetTokenIf(TokenType.WHERE))
			{
				//read expressions [column] > number  separated by AND|OR
				// [number | string | identifier] [oper] [number | string | identifier] [AND | OR]

				bool endOfWhere = false;
				bool identifierExpected = true;
				while (!endOfWhere)
				{
					if (identifierExpected)
					{
						//read left
						var left = ReadOperand();

						//read operator
						if (!GetToken() || !CurrentToken.IsOperator)
						{
							throw new ArgumentException("operator of WHERE expression expected");
						}
						var oper = CurrentToken;

						//read right
						var right = ReadOperand();

						where.Add(new Expression(
							//tableCollection,
							left,
							oper.Token,
							right
						));
						identifierExpected = false;
					}
					else
					{
						//end of where if not a logical
						if (PeekToken() && peekedToken.IsLogical)
						{
							//consume it
							GetToken();

							where.Add(new LogicalExpression(CurrentToken.Token));

							//next must be an identifier
							identifierExpected = true;
						}
						else
						{
							endOfWhere = true;
						}

					}
				}
				//a WHERE empty is invalid
				if (where.Count == 0)
				{
					throw new ArgumentException("WHERE expression(s) expected");
				}
			}

			#endregion

			#region LIMIT

			//read LIMIT integer if any
			if (GetTokenIf(TokenType.LIMIT))
			{
				//read integer
				if (!GetToken() ||
					CurrentToken.Token != TokenType.Number ||
					!int.TryParse(CurrentToken.Value, out limitValue))
				{
					throw new ArgumentException("Integer number after LIMIT expected");
				}
			}

			#endregion

			if (GetToken() && CurrentToken.Token != TokenType.SemiColon)
			{
				throw new ArgumentException($"Unexpected: {CurrentToken.Value} @ {CurrentToken.Position}");
			}

			return new DbQuery(columnSelect, tableCollection, join, where, limitValue);
		}

	}
}
