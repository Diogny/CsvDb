using System;
using System.Collections.Generic;
using System.Text;
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

		static string identifierChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789";

		static string numberChars = "0123456789";

		static string[] keyWords = new string[] { "SELECT", "FROM", "WHERE", "SKIP", "LIMIT",
			"AND", "OR",
			"Byte", "Int16", "Int32", "String", "Double" };

		bool CanRead { get { return index < length; } }

		StringBuilder charString = new StringBuilder();

		List<TokenStruct> list = new List<TokenStruct>();

		public enum Token
		{
			SELECT,
			FROM,
			WHERE,
			SKIP,
			LIMIT,
			AND,
			OR,
			//table column, variable
			Identifier,
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

		string ReadIdentifier()
		{
			var sb = new StringBuilder();
			sb.Append(currentChar);

			while (identifierChars.IndexOf(PeekChar()) >= 0)
			{
				sb.Append(ReadChar());
			}
			return sb.ToString();
		}

		string ReadNumber()
		{
			var sb = new StringBuilder();
			sb.Append(currentChar);
			//
			while (numberChars.IndexOf(PeekChar()) >= 0)
			{
				sb.Append(ReadChar());
			}
			if (PeekChar() == '.')
			{
				sb.Append(ReadChar());
				//
				while (numberChars.IndexOf(PeekChar()) >= 0)
				{
					sb.Append(ReadChar());
				}
			}
			return sb.ToString();
		}

		public CsvDbQuery Parse(CsvDb db, string query)
		{
			Reset();
			TokenStruct token;
			var enumerator = ParseTokens(query).GetEnumerator();

			CsvDbTable table = null;
			CsvDbQueryColumnSelector colSelector = null;
			var where = new List<CsvDbQueryExpressionBase>();
			int skipValue = -1;
			int limitValue = -1;

			//read SELECT
			if (!enumerator.MoveNext() || (token = enumerator.Current).Token != Token.SELECT)
			{
				throw new ArgumentException("SELECT expected");
			}

			//read column selector: comma separated or *
			if (!enumerator.MoveNext())
			{
				throw new ArgumentException("table column selector definition expected");
			}
			var selectors = new List<TokenStruct>();
			bool endOfStream = false;
			bool isFull = false;

			if (enumerator.Current.Token == Token.Astherisk)
			{
				isFull = true;
				selectors.Add(enumerator.Current);
				//
				endOfStream = !enumerator.MoveNext();
			}
			else if (enumerator.Current.Token == Token.Identifier)
			{
				selectors.Add(enumerator.Current);

				//comma separated columns
				while (!(endOfStream = !enumerator.MoveNext()) &&
					enumerator.Current.Token == Token.Comma)
				{
					//don't add commas, just table column name identifiers
					//test for next identifier
					if (!enumerator.MoveNext() || enumerator.Current.Token != Token.Identifier)
					{
						throw new ArgumentException("table column selector definition expected");
					}
					//add identifier
					selectors.Add(enumerator.Current);
				}
			}
			else
			{
				throw new ArgumentException("table column selector definition expected");
			}

			//read FROM
			if (endOfStream || (token = enumerator.Current).Token != Token.FROM)
			{
				throw new ArgumentException("FROM expected");
			}

			//read identifier: table name
			if (!enumerator.MoveNext() || (token = enumerator.Current).Token != Token.Identifier)
			{
				throw new ArgumentException("table name identifier after FROM expected");
			}

			//first check -table name
			table = db.Table(token.Value);
			if (table == null)
			{
				throw new ArgumentException($"Cannot find table [{token.Value}] in database.");
			}

			//second check -table columns
			var columns = new List<CsvDbColumn>();
			if (selectors[0].Token != Token.Astherisk)
			{
				foreach (var tk in selectors)
				{
					var c = table.Column(tk.Value);
					if (c == null)
					{
						throw new ArgumentException($"Cannot find {tk.Value} in [{table.Name}] name of database.");
					}
					columns.Add(c);
				}
			}
			else
			{
				columns = table.Columns;
			}
			colSelector = new CsvDbQueryColumnSelector(columns, isFull);

			//read WHERE if any
			if (!(endOfStream = !enumerator.MoveNext()) &&
				(token = enumerator.Current).Token == Token.WHERE)
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
						if (!enumerator.MoveNext() || !(token = enumerator.Current).IsIdentifier)
						{
							throw new ArgumentException("left operand of WHERE expression expected");
						}
						var left = token;

						//read operator
						if (!enumerator.MoveNext() || !(token = enumerator.Current).IsOperator)
						{
							throw new ArgumentException("operator of WHERE expression expected");
						}
						var oper = token;

						//read right
						if (!enumerator.MoveNext() || !(token = enumerator.Current).IsIdentifier)
						{
							throw new ArgumentException("right operand of WHERE expression expected");
						}
						var right = token;

						where.Add(new CsvDbQueryExpression(
							table,
							left.Value,
							oper.Value,
							right.Value
						));
						identifierExpected = false;
					}
					else
					{
						//end of where if not a logical
						if (!(endOfStream = !enumerator.MoveNext()) && (token = enumerator.Current).IsLogical)
						{
							where.Add(new CsvDbQueryLogical(token.Value));

							//next must be an identifier
							identifierExpected = true;
						}
						else
						{
							endOfWhere = true;
						}

					}
				}
				//
				if (where.Count == 0)
				{
					throw new ArgumentException("WHERE expression(s) expected");
				}
			}

			//read SKIP integer if any
			if (!endOfStream && token.Token == Token.SKIP)
			{
				//read integer
				if (!enumerator.MoveNext() ||
					(token = enumerator.Current).Token != Token.Number ||
					!int.TryParse(enumerator.Current.Value, out skipValue))
				{
					throw new ArgumentException("Integer number after SKIP expected");
				}
			}

			//read LIMIT integer if any
			if (enumerator.MoveNext() && (token = enumerator.Current).Token == Token.LIMIT)
			{
				//read integer
				if (!enumerator.MoveNext() ||
					(token = enumerator.Current).Token != Token.Number ||
					!int.TryParse(enumerator.Current.Value, out limitValue))
				{
					throw new ArgumentException("Integer number after LIMIT expected");
				}
			}

			//should be end of stream
			if (enumerator.MoveNext())
			{
				throw new ArgumentException("End of Db Query reached and extra tokens remaings");
			}

			return new CsvDbQuery(db, query, table, colSelector, where, skipValue, limitValue);
		}

	}
}
