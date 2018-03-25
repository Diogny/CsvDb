using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using static CsvDb.CsvDbQuery;

namespace CsvDb
{
	public class CsvDbQueryParser
	{
		int index;
		int startOfToken;
		char[] queryBuffer;
		int length;
		char currentChar;

		//static string identifierChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789";

		//static string numberChars = "0123456789";

		static string[] keyWords = new string[] { "SELECT", "FROM", "WHERE", "SKIP", "LIMIT",
			"AND", "OR",
			"Byte", "Int16", "Int32", "String", "Double" };

		bool CanRead { get { return index < length; } }

		StringBuilder charString = new StringBuilder();

		List<TokenStruct> list = new List<TokenStruct>();

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
			//table column, variable, descriptor
			Identifier,
			//[identifier as table name].[column name]
			ColumnIdentifier,
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
			Comma
		}

		public struct TokenStruct
		{
			public Token Token;
			public String Value;
			public int Position;

			//[number | string | identifier]
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

			public override string ToString() => $"({Token}){Value}";
		}

		void Reset()
		{
			index = 0;
			currentChar = (char)0;
			startOfToken = -1;
			charString.Length = 0;
			list.Clear();
		}

		protected internal IEnumerable<TokenStruct> ParseTokens(string query)
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
							case '=':
								AddToken(Token.LessOrEqual, text: null, readNext: true);
								break;
							case '>':
								AddToken(Token.NotEqual, text: null, readNext: true);
								break;
							default:
								AddToken(Token.Less);
								break;
						}
						break;
					case '>':
						if (PeekChar() == '=')
						{
							AddToken(Token.GreaterOrEqual, text: null, readNext: true);
						}
						else
						{
							AddToken(Token.Greater);
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
								if (Enum.TryParse<Token>(str, out Token result))
								{
									AddToken(result, text: str);
								}
								else
								{
									AddToken(Token.Identifier, text: str);
								}
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

		TokenStruct AddToken(Token token, String text = null, bool readNext = false)
		{
			if (readNext)
			{
				ReadChar();
			}
			if (text == null)
			{
				text = charString.ToString();
			}
			var tk = new TokenStruct()
			{
				Token = token,
				Value = text,
				Position = startOfToken
			};
			list.Add(tk);
			charString.Length = 0;
			startOfToken = -1;
			//
			return tk;
		}

		char ReadChar(
			bool recordStart = false,
			bool skipCanRead = false,
			bool peek = false,
			bool append = true)
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

		//string ReadIdentifier()
		//{
		//	var sb = new StringBuilder();
		//	sb.Append(currentChar);

		//	while (identifierChars.IndexOf(PeekChar()) >= 0)
		//	{
		//		sb.Append(ReadChar());
		//	}
		//	return sb.ToString();
		//}

		//string ReadNumber()
		//{
		//	var sb = new StringBuilder();
		//	sb.Append(currentChar);
		//	//
		//	while (numberChars.IndexOf(PeekChar()) >= 0)
		//	{
		//		sb.Append(ReadChar());
		//	}
		//	if (PeekChar() == '.')
		//	{
		//		sb.Append(ReadChar());
		//		//
		//		while (numberChars.IndexOf(PeekChar()) >= 0)
		//		{
		//			sb.Append(ReadChar());
		//		}
		//	}
		//	return sb.ToString();
		//}

		public CsvDbQuery Parse(CsvDb db, string query)
		{
			Reset();
			TokenStruct PreviousToken = default(TokenStruct);
			TokenStruct CurrentToken = default(TokenStruct);
			var peekedToken = default(TokenStruct);

			var queue = new Queue<TokenStruct>(ParseTokens(query));

			CsvDbTable table = null;
			var tableCollection = new List<CsvDbQueryTableIdentifier>();
			//
			CsvDbQueryColumnSelector colSelector = null;
			var where = new List<CsvDbQueryExpressionBase>();
			int skipValue = -1;
			int limitValue = -1;
			CsvDbQueryTableIdentifier tableIdentifier = null;
			bool EndOfStream = queue.Count == 0;
			var columnSelectors = new List<TokenStruct>();
			bool isFullColums = false;

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
				if (EndOfStream || (EndOfStream = !queue.TryDequeue(out TokenStruct tk)))
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

			//Func<string, TokenStruct> ReadIdentifier = (errmsg) =>
			TokenStruct ReadOperand(string errmsg)
			{
				if (!GetToken() || !CurrentToken.IsIdentifier)
				{
					throw new ArgumentException(errmsg);
				}
				var identifier = CurrentToken.Value;
				var position = CurrentToken.Position;
				var tokenType = CurrentToken.Token;
				var dot = String.Empty;
				var columnName = String.Empty;

				//try to peek a dot . after identifier
				if (tokenType == Token.Identifier && GetTokenIf(Token.Dot))
				{
					dot = CurrentToken.Value;  //  .
					tokenType = Token.ColumnIdentifier;

					//read column identifier
					if (!GetToken() || !CurrentToken.IsIdentifier)
					{
						throw new ArgumentException("missing column name after table name identifier");
					}
					columnName = CurrentToken.Value;
				}
				var newToken = new TokenStruct()
				{
					Token = tokenType,
					Value = $"{identifier}{dot}{columnName}",
					Position = position
				};
				return newToken;
			}

			//read SELECT
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

			//read FROM
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
				tableIdentifier = new CsvDbQueryTableIdentifier(table, CurrentToken.Value);
			}
			else
			{
				tableIdentifier = new CsvDbQueryTableIdentifier(table);
			}
			tableCollection.Add(tableIdentifier);

			var columns = new List<CsvDbQueryColumnIdentifier>();

			if (columnSelectors[0].Token != Token.Astherisk)
			{
				//unresolved column identifiers can exists for more complex queries
				//	later add unresolved features
				foreach (var tk in columnSelectors)
				{
					string columnName = tk.Value;
					string identifier = String.Empty;

					if (tk.Token == Token.ColumnIdentifier)
					{
						if (!tk.Parse(out identifier, out columnName))
						{
							throw new ArgumentException($"invalid column identifier: {tk.Value}");
						}
						//look only inside the table of the identifier
						var tble = tableCollection
							.Where(t => t.HasIdentifier)
							.FirstOrDefault(t => t.Identifier == identifier);

						var col = tble?.Table.Columns.FirstOrDefault(cl => cl.Name == columnName);
						if (col == null)
						{
							throw new ArgumentException($"invalid column identifier: {tk.Value}");
						}
						//add column with identifier
						columns.Add(new CsvDbQueryColumnIdentifier(col, identifier));
					}
					else
					{
						//look in all tables
						var col = db.Tables
							.SelectMany(t => t.Columns)
							.FirstOrDefault(c => c.Name == columnName);

						if (col == null)
						{
							throw new ArgumentException($"Cannot find {tk.Value} in any table of database.");
						}
						columns.Add(new CsvDbQueryColumnIdentifier(col));
					}
				}
			}
			else
			{
				columns = table.Columns
					.Select(c => new CsvDbQueryColumnIdentifier(c))
					.ToList();
			}
			colSelector = new CsvDbQueryColumnSelector(columns, isFullColums);

			//read WHERE if any
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

						where.Add(new CsvDbQueryExpression(
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

							where.Add(new CsvDbQueryLogical(CurrentToken.Value));

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

			//should be end of stream
			if (GetToken())
			{
				throw new ArgumentException("End of Db Query reached and extra tokens remaings");
			}

			return new CsvDbQuery(db, query, tableIdentifier, colSelector, where, skipValue, limitValue);
		}

	}
}
