using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb.Query
{
	public class DbQueryParser
	{
		int index;
		int startOfToken;
		char[] queryBuffer;
		int length;
		char currentChar;

		//static string identifierChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789";

		//static string numberChars = "0123456789";

		static List<string> keyWords = new List<string> {
			"SELECT", "FROM", "WHERE", "SKIP", "LIMIT",
			"AND", "OR", "NOT",
			"JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "FULL", "CROSS",
			"ON", "IS", "NULL", "EXISTS",
			"COUNT", "AVG", "SUM" };

		static List<string> castingTypes = new List<string> {
			//numeric
			"Byte", "Int16", "Int32", "Int64", "Single", "Double", "Decimal",
			//char/string
			"Char", "String"
		};

		bool CanRead { get { return index < length; } }

		internal static List<Token> JoinStarts = new List<Token>()
		{
			Token.INNER, Token.LEFT, Token.RIGHT, Token.FULL, Token.CROSS
		};

		internal static List<Token> SelectQuantifiers = new List<Token>()
		{
			Token.COUNT, Token.AVG, Token.SUM
		};

		StringBuilder charString = new StringBuilder();

		List<TokenItem> list = new List<TokenItem>();

		void Reset()
		{
			index = 0;
			currentChar = (char)0;
			startOfToken = -1;
			charString.Length = 0;
			list.Clear();
		}

		internal IEnumerable<TokenItem> ParseTokens(string query)
		{
			if (String.IsNullOrWhiteSpace(query))
			{
				throw new ArgumentException("Invalid query");
			}

			queryBuffer = query.ToCharArray();
			length = queryBuffer.Length;

			while (CanRead)
			{
				switch (ReadChar(recordStart: true))
				{
					case '.':
						AddToken(Token.Dot);
						break;
					case ',':
						AddToken(Token.Comma);
						break;
					case ';':
						AddToken(Token.SemiColon);
						break;
					case '*':
						AddToken(Token.Astherisk);
						break;
					case '(':
						AddToken(Token.OpenPar);
						break;
					case ')':
						AddToken(Token.ClosePar);
						break;
					case '=':
						AddToken(Token.Equal);
						break;
					case '<':
						switch (PeekChar())
						{
							case '=':  // <=
								AddToken(Token.LessOrEqual, text: null, readNext: true);
								break;
							case '>':   // <>
								AddToken(Token.NotEqual, text: null, readNext: true);
								break;
							default:   // <
								AddToken(Token.Less);
								break;
						}
						break;
					case '>':
						if (PeekChar() == '=')
						{
							AddToken(Token.GreaterOrEqual, text: null, readNext: true);   // >=
						}
						else
						{
							AddToken(Token.Greater);   // >
						}
						break;
					case '"':
						//string
						bool endOfString = false;
						bool endOfStream = false;

						while (!(endOfStream = !CanRead) && !endOfString)
						{
							//read as many as possible
							while (!(endOfStream = !CanRead) && ReadChar(skipCanRead: true, append: false) != '"')
							{
								charString.Append(currentChar);
							}
							if (endOfStream)
							{
								throw new ArgumentException($"Expected string terminator on pos:{index - 1}");
							}
							//we got a " store it
							charString.Append(currentChar);

							if (PeekChar() == '"')
							{
								//store "" and continue
								ReadChar();
								//charString.Append($"{currentChar}{ReadChar(append: false)}");
							}
							else
							{
								//end of string reached, test for last "
								if (charString[charString.Length - 1] != '"')
								{
									throw new ArgumentException($"End of stream reached while reading string identifier");
								}
								endOfString = true;
								//charString.Append("\"");
							}
						}
						AddToken(Token.String);
						break;
					default:
						if (char.IsDigit(currentChar))
						{
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
							AddToken(Token.Number);
						}
						else if ((int)currentChar <= 32)
						{
							//discard spaces
							charString.Length = 0;
						}
						else
						{
							bool isIdentifier = char.IsLetter(currentChar) || currentChar == '_';
							if (isIdentifier)
							{
								//identifier
								while (isIdentifier)
								{
									//charString.Append(currentChar);
									var peek = PeekChar();
									if ((isIdentifier = char.IsLetter(peek) || peek == '_'))
									{
										ReadChar();
									}
								}
								//
								var str = charString.ToString();

								var ndx = keyWords.IndexOf(str);
								if (ndx >= 0)
								{
									//key word
									AddToken((Token)(ndx + 1), text: str);
								}
								else if ((ndx = castingTypes.IndexOf(str)) >= 0)
								{
									//cast type
									AddToken(Token.CastType, text: str);
								}
								else
								{
									//an identifier
									AddToken(Token.Identifier, text: str);
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
			return list;
		}

		TokenItem AddToken(Token token, String text = null, bool readNext = false)
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
			list.Add(tk);
			charString.Length = 0;
			startOfToken = -1;
			//
			return tk;
		}

		char ReadChar(bool recordStart = false, bool skipCanRead = false, bool peek = false, bool append = true)
		{
			if (skipCanRead || CanRead)
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

		char PeekChar() { return ReadChar(peek: true, append: false); }

		public DbQuery Parse(CsvDb db, string query)
		{
			Reset();
			TokenItem PreviousToken = default(TokenItem);
			TokenItem CurrentToken = default(TokenItem);
			var peekedToken = default(TokenItem);

			var queue = new Queue<TokenItem>(ParseTokens(query));

			DbTable table = null;
			var tableCollection = new List<DbQueryTableIdentifier>();
			//
			DbQueryColumnSelector colSelector = null;
			var where = new List<DbQueryExpressionBase>();
			int skipValue = -1;
			int limitValue = -1;
			DbQueryTableIdentifier tableIdentifier = null;
			bool EndOfStream = queue.Count == 0;
			var columnSelectors = new List<TokenItem>();
			bool isFullColums = false;
			Token quantifier = Token.None;

			// SELECT * | column0,column1,...
			//					|	a.agency_id, b.serice_id,...
			//
			//	FROM table [descriptor]
			//		
			//	WHERE [descriptor].column = constant AND|OR ...
			//	SKIP number
			//	LIMIT number
			//

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

			bool GetTokenIf(Token token)
			{
				if (!EndOfStream && queue.TryPeek(out peekedToken) && peekedToken.Token == token)
				{
					CurrentToken = queue.Dequeue();
					return true;
				}
				return false;
			};

			bool GetTokenIfContains(List<Token> tokens)
			{
				if (!EndOfStream && queue.TryPeek(out peekedToken) && tokens.Contains(peekedToken.Token))
				{
					CurrentToken = queue.Dequeue();
					return true;
				}
				return false;
			}

			//Func<string, TokenStruct> ReadIdentifier = (errmsg) =>
			TokenItem ReadOperand(string errmsg, bool realDotIdentifier = false)
			{
				if (!GetToken() || !CurrentToken.IsIdentifier ||
					//this ensures it must be an identifier
					(realDotIdentifier && CurrentToken.Token != Token.Identifier))
				{
					throw new ArgumentException(errmsg);
				}
				var identifier = CurrentToken.Value;
				var position = CurrentToken.Position;
				TokenItem newToken = null;

				//try to peek a dot . after identifier
				if (CurrentToken.Token == Token.Identifier && GetTokenIf(Token.Dot))
				{
					//read column identifier
					if (!GetToken() || !CurrentToken.IsIdentifier)
					{
						throw new ArgumentException("missing column name after table name identifier");
					}
					newToken = new TokenItemIdentifier(CurrentToken.Value, position, identifier);
				}
				else if (realDotIdentifier)
				{
					throw new ArgumentException(errmsg);
				}
				else
				{
					newToken = new TokenItem(CurrentToken.Token, identifier, position);
				}
				return newToken;
			}

			#region SELECT

			if (!GetTokenIf(Token.SELECT))
			{
				throw new ArgumentException("SELECT expected");
			}

			//read column selector: comma separated or *
			if (GetTokenIf(Token.Astherisk))
			{
				isFullColums = true;
				columnSelectors.Add(CurrentToken);
			}
			else if (GetTokenIfContains(SelectQuantifiers))
			{
				quantifier = CurrentToken.Token;

				if (!GetTokenIf(Token.OpenPar))
				{
					throw new ArgumentException($"expected ( after {CurrentToken.Token}");
				}

				if (GetTokenIf(Token.Astherisk))
				{
					// COUNT(*)
					columnSelectors.Add(new TokenItem(Token.Astherisk, "*", CurrentToken.Position));
				}
				else
				{
					columnSelectors.Add(ReadOperand($"cannot read quantifier {quantifier}"));
				}

				if (!GetTokenIf(Token.ClosePar))
				{
					throw new ArgumentException($"expected ) closing {CurrentToken.Token}");
				}
			}
			else
			{
				//mut have at least one column identifier
				if (PeekToken() && peekedToken.Token != Token.Identifier)
				{
					throw new ArgumentException("table column name(s) expected");
				}
				//read first
				columnSelectors.Add(ReadOperand("cannot read column name"));

				while (GetTokenIf(Token.Comma))
				{
					//next
					columnSelectors.Add(ReadOperand("cannot read column name"));
				}
			}

			#endregion

			#region FROM

			if (!GetTokenIf(Token.FROM))
			{
				throw new ArgumentException("FROM expected");
			}

			//read identifier: table name
			if (!GetTokenIf(Token.Identifier))
			{
				throw new ArgumentException("table name identifier after FROM expected");
			}

			//check -table name
			table = db.Table(CurrentToken.Value);
			if (table == null)
			{
				throw new ArgumentException($"Cannot find table [{CurrentToken.Value}] in database.");
			}

			//there can be a table identifier before the WHERE and after table name
			if (GetTokenIf(Token.Identifier))
			{
				//create it so WHERE can handle it
				tableIdentifier = new DbQueryTableIdentifier(table, CurrentToken.Value);
			}
			else
			{
				tableIdentifier = new DbQueryTableIdentifier(table);
			}
			tableCollection.Add(tableIdentifier);

			if (isFullColums) //(columnSelectors[0].Token == Token.Astherisk)
			{
				colSelector = new DbQueryColumnSelector(table);
			}
			else if (quantifier != Token.None)
			{
				if (quantifier == Token.COUNT)
				{
					// COUNT (*)
					colSelector = new DbQueryColumnSelector(quantifier, table);
				}
				else
				{
					// AVG (column)  SUM (column)  where column:numeric
					colSelector = new DbQueryColumnSelector(quantifier,
						new DbQueryColumnIdentifier(table, columnSelectors[0].Value));
				}
			}
			else
			{
				var columns = new List<DbQueryColumnIdentifier>();

				//unresolved column identifiers can exists for more complex queries
				//	later add unresolved features
				foreach (var tk in columnSelectors)
				{
					if (tk.Token == Token.ColumnIdentifier)
					{
						var tkIdent = (TokenItemIdentifier)tk;

						//look only inside the table of the identifier
						var tble = tableCollection
							.Where(t => t.HasIdentifier)
							.FirstOrDefault(t => t.Identifier == tkIdent.Identifier);

						var col = tble?.Table.Columns.FirstOrDefault(cl => cl.Name == tkIdent.Value);
						if (col == null)
						{
							throw new ArgumentException($"invalid column identifier: {tkIdent.Value}");
						}
						//add column with identifier
						columns.Add(new DbQueryColumnIdentifier(col, tkIdent.Identifier));
					}
					else
					{
						//look in all tables
						var colsFound = db.Tables
							.SelectMany(t => t.Columns)
							.Where(c => c.Name == tk.Value)
							.ToList();

						if (colsFound.Count == 0)
						{
							throw new ArgumentException($"Cannot resolve column {tk.Value} in any table");
						}
						else if (colsFound.Count > 1)
						{
							throw new ArgumentException($"cannot resolve column: {tk.Value} on multiple tables: {String.Join(", ", colsFound.Select(c => c.Name))}");
						}
						//get first one
						columns.Add(new DbQueryColumnIdentifier(colsFound[0]));
					}
				}
				colSelector = new DbQueryColumnSelector(columns);
			}

			#endregion

			#region JOIN

			SqlJoin sqlJoin = null;
			if (GetTokenIfContains(JoinStarts))
			{
				//FROM table must has identifier, should be only one here
				if (tableCollection.Any(t => !t.HasIdentifier))
				{
					//throw new ArgumentException("JOIN queries FROM table must have identifier");
				}

				Token JoinCommand = CurrentToken.Token;
				DbQueryExpression JoinExpression = null;
				DbQueryTableIdentifier joinTable = null;

				if (CurrentToken.Token == Token.LEFT || CurrentToken.Token == Token.RIGHT ||
					CurrentToken.Token == Token.FULL)
				{
					//LEFT OUTER JOIN
					//RIGHT OUTER JOIN
					//FULL OUTER JOIN
					if (!GetTokenIf(Token.OUTER))
					{
						throw new ArgumentException($"OUTER expected after {CurrentToken.Token}");
					}
				}
				else if (!(CurrentToken.Token == Token.INNER || CurrentToken.Token == Token.CROSS))
				{
					//INNER JOIN
					//CROSS JOIN
					throw new ArgumentException($"invalid JOIN keyword {CurrentToken.Token}");
				}

				if (!GetTokenIf(Token.JOIN))
				{
					throw new ArgumentException($"JOIN expected after {CurrentToken.Token}");
				}

				//read table name
				if (!GetTokenIf(Token.Identifier))
				{
					throw new ArgumentException("table name identifier after FROM expected");
				}
				//check -table name
				table = db.Table(CurrentToken.Value);
				if (table == null)
				{
					throw new ArgumentException($"Cannot find table [{CurrentToken.Value}] in database.");
				}
				// + identifier
				if (GetTokenIf(Token.Identifier))
				{
					tableCollection.Add(joinTable = new DbQueryTableIdentifier(table, CurrentToken.Value));
				}

				//read ON
				if (!GetTokenIf(Token.ON))
				{
					throw new ArgumentException($"ON expected in JOIN query");
				}

				//read expression
				//left & right operand must be from different tables
				//read left
				var left = ReadOperand("left operand of JOIN expression expected", realDotIdentifier: true);

				//read operator
				if (!GetToken() || !CurrentToken.IsOperator)
				{
					throw new ArgumentException("operator of JOIN expression expected");
				}
				var oper = CurrentToken;

				//read right
				var right = ReadOperand("right operand of JOIN expression expected", realDotIdentifier: true);

				if (((TokenItemIdentifier)left).Identifier == ((TokenItemIdentifier)right).Identifier)
				{
					throw new ArgumentException("JOIN query expression table identifiers cannot be the same");
				}

				JoinExpression = new DbQueryExpression(
					tableCollection,
					left,
					oper.Value,
					right
				);
				sqlJoin = new SqlJoin(JoinCommand, joinTable, JoinExpression);
			}

			#endregion

			#region WHERE if any

			if (GetTokenIf(Token.WHERE))
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
						var left = ReadOperand("left operand of WHERE expression expected");

						//read operator
						if (!GetToken() || !CurrentToken.IsOperator)
						{
							throw new ArgumentException("operator of WHERE expression expected");
						}
						var oper = CurrentToken;

						//read right
						var right = ReadOperand("right operand of WHERE expression expected");

						where.Add(new DbQueryExpression(
							tableCollection,
							left,
							oper.Value,
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

							where.Add(new DbQueryLogical(CurrentToken.Value));

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

			#region SKIP & LIMIT

			//read SKIP integer if any
			if (GetTokenIf(Token.SKIP))
			{
				//read integer
				if (!GetToken() ||
					CurrentToken.Token != Token.Number ||
					!int.TryParse(CurrentToken.Value, out skipValue))
				{
					throw new ArgumentException("Integer number after SKIP expected");
				}
			}

			//read LIMIT integer if any
			if (GetTokenIf(Token.LIMIT))
			{
				//read integer
				if (!GetToken() ||
					CurrentToken.Token != Token.Number ||
					!int.TryParse(CurrentToken.Value, out limitValue))
				{
					throw new ArgumentException("Integer number after LIMIT expected");
				}
			}

			#endregion

			//should be end of stream
			if (GetToken())
			{
				throw new ArgumentException("End of Db Query reached and extra tokens remaings");
			}

			return new DbQuery(db, query, colSelector, tableIdentifier, tableCollection,
				sqlJoin, where, skipValue, limitValue);
		}

	}

	public enum Token
	{
		None,
		SELECT,
		FROM,
		WHERE,
		SKIP,
		LIMIT,
		AND,
		OR,
		NOT,
		JOIN,
		INNER,
		LEFT,
		RIGHT,
		OUTER,
		FULL,
		CROSS,
		ON,
		IS,
		NULL,
		EXISTS,
		COUNT,
		AVG,
		SUM,
		//table column, variable, descriptor
		Identifier,
		//[identifier as table name].[column name]
		ColumnIdentifier,
		//"Byte", "Int16", "Int32", "String", "Double"
		CastType,
		//
		Number,
		//
		String,
		// = > >= < <= <>
		Equal,
		NotEqual,
		Greater,
		GreaterOrEqual,
		Less,
		LessOrEqual,
		// (
		OpenPar,
		// )
		ClosePar,
		// .
		Dot,
		// *
		Astherisk,
		// ,
		Comma,
		// ;  it's the end of an SQL query
		SemiColon
	}

	public class TokenItem
	{
		public Token Token { get; internal set; }
		public String Value { get; internal set; }
		public int Position { get; internal set; }

		public virtual bool HasIdentifier => false;

		/// <summary>
		/// Number, String, Identifier
		/// </summary>
		public bool IsIdentifier
		{
			get
			{
				return Token == Token.Number || Token == Token.String || Token == Token.Identifier;
			}
		}

		public bool IsOperator
		{
			get
			{
				return Token == Token.Equal || Token == Token.NotEqual ||
					Token == Token.Less || Token == Token.LessOrEqual ||
					Token == Token.Greater || Token == Token.GreaterOrEqual;
			}
		}

		public bool IsLogical
		{
			get
			{
				return Token == Token.AND || Token == Token.OR;
			}
		}

		internal TokenItem(Token token, string value, int position)
		{
			Token = token;
			Value = value;
			Position = position;
		}

		public override string ToString() => $"({Token}) {Value}";
	}

	public class TokenItemIdentifier : TokenItem
	{
		public override bool HasIdentifier => true;

		public string Identifier { get; internal set; }

		internal TokenItemIdentifier(string value, int position, string identifier) :
			base(Token.ColumnIdentifier, value, position)
		{
			Identifier = identifier;
		}

		public override string ToString() => $"({Token}) {Identifier}.{Value}";
	}


}
