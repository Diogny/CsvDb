using System;
using System.Collections.Generic;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// Implements a command line parser
	/// </summary>
	public class CommandLineParser
	{
		/// <summary>
		/// Gets the original text
		/// </summary>
		public string Text { get; }

		/// <summary>
		/// Gets the parsed arguments
		/// </summary>
		public string[] Args { get; }

		/// <summary>
		/// True if empty args
		/// </summary>
		public bool Empty => Args.Length == 0;

		/// <summary>
		/// Creates a command line parser
		/// </summary>
		/// <param name="text">command argment string</param>
		public CommandLineParser(string text)
		{
			Args = SplitArgs(Text = String.IsNullOrWhiteSpace(text) ? String.Empty : text);
		}

		//https://stackoverflow.com/questions/298830/split-string-containing-command-line-parameters-into-string-in-c-sharp?noredirect=1&lq=1

		public static bool IsQuote(char ch) => ch == '\'' || ch == '"';

		/// <summary>
		/// Enumerate all command argument values
		/// </summary>
		/// <param name="args">array of string arguments</param>
		/// <returns></returns>
		public static IEnumerable<CommandArgValue> EnumerateArguments(string[] args)
		{
			if (args == null || args.Length == 0)
			{
				yield break;
			}
			var index = 0;
			String key = null;
			String value = null;

			bool GetNext(out string next) =>
				(next = (index < args.Length) ? args[index++] : null) != null;

			bool GetNextIf(string next) =>
				(next = (index < args.Length && args[index] == next) ? args[index++] : null) != null;

			while (GetNext(out key))
			{
				//try next for key pair char :
				if (GetNextIf(":"))
				{
					if (!GetNext(out value))
					{
						throw new ArgumentException($"missing value after: {key}:");
					}
					yield return new CommandArgKeypair(key, value);
				}
				else
				{
					yield return new CommandArgValue(key);
				}
			}
		}

		/// <summary>
		/// Enumerate all command argument values
		/// </summary>
		/// <returns></returns>
		public IEnumerable<CommandArgValue> Arguments()
		{
			return EnumerateArguments(Args);
		}

		/// <summary>
		/// Split an command argument text string into an argument array
		/// </summary>
		/// <param name="argsString">command argument text string</param>
		/// <returns></returns>
		public static string[] SplitArgs(string argsString)
		{
			char[] parmChars = argsString.ToCharArray();
			int length = parmChars.Length;
			var ndx = 0;
			var emptyChar = (char)0;
			var currChar = emptyChar;
			var peekedChar = emptyChar;
			string identifier = null;

			bool endOfStream() => ndx >= length;

			var lines = new List<string>();

			bool PeekChar(out char ch)
			{
				return (ch = endOfStream() ? (char)0 : parmChars[ndx]) != emptyChar;
			}

			bool GetChar()
			{
				if (endOfStream())
				{
					return false;
				}
				currChar = parmChars[ndx++];
				return true;
			}

			bool GetCharIf(char ch)
			{
				return (peekedChar = endOfStream() ?
					(char)0 :
					(parmChars[ndx] == ch ? parmChars[ndx++] : emptyChar)
				) != emptyChar;
			}

			string ReadIdentifier(Func<char, bool> startChar, Func<char, bool> nextChars)
			{
				var startIndex = ndx;
				//must start with a letter
				if (!startChar(parmChars[ndx]))
				{
					return String.Empty;
				}
				ndx++;
				//and can follow letters and digits
				while (!endOfStream() && nextChars(parmChars[ndx]))
				{
					ndx++;
				}
				//
				return new string(parmChars, startIndex, ndx - startIndex);
			}

			string ReadString(char quote, bool removeWrappingQuote = false)
			{
				//ndx -1 to consume starting quote char
				var startIndex = ndx - 1;
				bool end = false;
				var oppositeQuote = (quote == '\'') ? '"' : '\'';

				while (!end && GetChar())
				{
					switch (currChar)
					{
						case '\\':
							if (!GetChar())
							{
								throw new ArgumentException($"after \\ there must be a char");
							}
							if (IsQuote(currChar))
							{
								//read until next \ + quote
								var endQuote = currChar;
								var endQuoted = false;
								while (!(endOfStream() || endQuoted) && GetChar())
								{
									endQuoted = currChar == '\\' && GetChar() && (currChar == endQuote);
								}
							}
							break;
						default:
							end = (currChar == quote);
							break;
					}
				}
				if (!end)
				{
					return String.Empty;
				}
				return new string(parmChars, startIndex, ndx - startIndex);
			}

			while (GetChar())
			{
				switch (currChar)
				{
					case '/':
						//UNIX /f8G  /f   /f_07
						if (String.IsNullOrWhiteSpace(identifier =
							ReadIdentifier(
								(ch) => Char.IsLetter(ch),
								(ch) => Char.IsLetterOrDigit(ch) || ch == '_')))
						{
							throw new ArgumentException($"  /options invalid");
						}
						//add new line
						lines.Add($"/{identifier}");
						break;
					case '\'':
					case '"':
						//read string
						if (String.IsNullOrWhiteSpace(identifier = ReadString(currChar)))
						{
							throw new ArgumentException($"invalid string");
						}
						//add new line
						lines.Add(identifier);
						break;
					case '.':
					case ':':
						//control chars
						//for agency.agency_id
						lines.Add(currChar.ToString());
						break;
					case '-':
						// -o  -O  --opt ---f   --_for
						int hypenCount = 1;
						while (GetCharIf('-'))
						{
							hypenCount++;
						}
						//try to read next string
						if (String.IsNullOrWhiteSpace(identifier =
							ReadIdentifier(
								(ch) => Char.IsLetter(ch) || ch == '_',
								(ch) => Char.IsLetterOrDigit(ch) || ch == '_')))
						{
							throw new ArgumentException($"  -- options without identifier");
						}
						//add new line
						lines.Add($"{(new string('-', hypenCount))}{identifier}");
						break;
					default:
						if (Char.IsDigit(currChar))
						{
							ndx--;
							//read number
							if (String.IsNullOrWhiteSpace(identifier =
								ReadIdentifier((ch) => Char.IsDigit(ch), (ch) => Char.IsDigit(ch))))
							{
								throw new ArgumentException($" invalid number");
							}
							//add new line
							lines.Add(identifier);
						}
						else if (currChar > 32)
						{
							ndx--;
							//try identifier
							if (String.IsNullOrWhiteSpace(identifier =
								ReadIdentifier(
									(ch) => Char.IsLetter(ch) || currChar == '_',
									(ch) => Char.IsLetterOrDigit(ch) || ch == '_')))
							{
								//no, so read until next space if any
								if (String.IsNullOrWhiteSpace(identifier =
									ReadIdentifier(
										(ch) => ch > 32,
										(ch) => ch > 32)))
								{
									throw new ArgumentException($" empty text");
								}
							}
							//add new line
							lines.Add(identifier);
						}
						break;
				}
			}

			return lines.ToArray();
		}

	}

	//real scenarios
	// x dbname
	// m dbname				use dbname
	// k dbname				kill dbname
	// 
	// display /table:"agency" /column:"agency_id"
	// display /column:"agency.agency_id" --top:12 /offset:8
	//
	// d	display	
	// t	display /tables							:all tables
	//		display table_name					:only table-name
	//		display "table.column"			:table column
	//				/n	: node info					old: i table.column
	//				/i	: items info				old: n table.column
	//				/p /offset:2344					old: p table.column offset:integer
	//				/c oper:>= constant			old: c table.column comparer constant
	//   display "table.column" /c:>= "house"
	//  display "table.column" /n /offset:45678
	//
	// h		--help -h	help
	// q		--quit -q	quit
	// r		--clear -c

	// e  & enter sql query string
	// s  & enter sql query string
	//	search
	// v table.column count:integer
	// c table.column comparer constant
	//      comparer: == <> > >= < <=    constant:  integer, real, string, char
	// 
	//
	// file -i -b file.txt
	// file -z bar.txt.gz
	//
	// /src:"C:\tmp\Some Folder\Sub Folder" /users:"abcdefg@hijkl.com" tasks:"SomeTask,Some Other Task" -someParam foo
	// /src:"C:\tmp\Some Folder\Sub Folder" /users:"abcdefg@hijkl.com" tasks:"SomeTask,'Some' \"Other\" Task" -someParam foo
	//   --option 45 /f "a \"d\" d" /g08 -k14  
	//  agency  . agency_id  
	//  show /n agency.agency_id
	// "akak \"ddd\" dssd"
	// "dsdd 'ddd' dddd"
	// 'dcdc "dcdcfcf" dcdcd '
	// 'dcd \'dcdcd\' ddcd'
	// "sdsdsd ""sdsds"" ddcdc"
	// 'dcdced ''ddewdwe'' dwede'

	[Flags]
	public enum CommandArgItemType : int
	{
		/// <summary>
		/// None
		/// </summary>
		None = 0,
		/// <summary>
		/// delimited with string quotes
		/// </summary>
		String = 1,
		/// <summary>
		/// A number
		/// </summary>
		Integer = 2,
		/// <summary>
		/// task
		/// </summary>
		Identifier = 4,
		/// <summary>
		/// -task	 --task
		/// </summary>
		Option = 8,
		/// <summary>
		/// /f		/src   /fG8
		/// </summary>
		Directive = 16,
		/// <summary>
		/// /Anything
		/// </summary>
		Chars = 32
	}

	/// <summary>
	/// Implements a command argument key value
	/// </summary>
	public class CommandArgValue
	{
		/// <summary>
		/// Gets the key value
		/// </summary>
		public string Key { get; }

		/// <summary>
		/// Gets the single unique non-flag type of the key value
		/// </summary>
		public CommandArgItemType Type { get; }

		/// <summary>
		/// True if it's a key pair
		/// </summary>
		public virtual bool IsKeyPair => false;

		/// <summary>
		/// Gets the command argument type of a text
		/// </summary>
		/// <param name="text">command argument</param>
		/// <returns></returns>
		public static CommandArgItemType GetType(string text)
		{
			if (String.IsNullOrWhiteSpace(text))
			{
				return CommandArgItemType.None;
			}
			else if (Int32.TryParse(text, out int number))
			{
				return CommandArgItemType.Integer;
			}
			else if (text.StartsWith('-'))
			{
				return CommandArgItemType.Option;
			}
			else if (text.StartsWith('/'))
			{
				return CommandArgItemType.Directive;
			}
			else if (CommandLineParser.IsQuote(text[0]) && text[0] == text[text.Length - 1])
			{
				return CommandArgItemType.String;
			}
			else if (Char.IsLetter(text[0]) || text[0] == '_')
			{
				return CommandArgItemType.Identifier;
			}
			else
			{
				return CommandArgItemType.Chars;
			}
		}

		/// <summary>
		/// Creates a command argument key value
		/// </summary>
		/// <param name="key">key value</param>
		public CommandArgValue(string key)
		{
			// task
			// -task			an option
			// --task
			// /f		/src	a directive
			// /fG8
			//
			Type = GetType(Key = key);
		}

		public override string ToString() => Key;
	}

	/// <summary>
	/// Implements a command argument key pair
	/// </summary>
	public class CommandArgKeypair : CommandArgValue
	{
		/// <summary>
		/// Gets the value of the key pair
		/// </summary>
		public string Value { get; }

		/// <summary>
		/// True if it's a key pair
		/// </summary>
		public override bool IsKeyPair => true;

		/// <summary>
		/// Gets the single unique non-flag type of the key pair value
		/// </summary>
		public CommandArgItemType ValueType { get; }

		/// <summary>
		/// Creates a command argument key value pair
		/// </summary>
		/// <param name="key">key value</param>
		/// <param name="value">value of the key</param>
		public CommandArgKeypair(string key, string value)
			: base(key)
		{
			//value can not be an Option nor a Directive
			//  Identifier | Integer | String
			// /f:34
			// --option:"dddd"
			// /f:for

			if ((ValueType = GetType(Value = value)) == CommandArgItemType.Option ||
				ValueType == CommandArgItemType.Directive)
			{
				throw new ArgumentException($"invalid command argument key pair: {this}");
			}
		}

		public override string ToString() => $"{Key}:{Value}";
	}

	/// <summary>
	/// Implements a command argument rule
	/// </summary>
	public class CommandArgRule
	{
		/// <summary>
		/// Unique id number inside a collection of rules in an Action
		/// </summary>
		public int Id { get; internal set; }

		/// <summary>
		/// Gets the command argument value, null if not matched
		/// </summary>
		public CommandArgValue Arg { get; private set; }

		/// <summary>
		/// Gets the matching condition for this rule
		/// </summary>
		public Func<CommandArgValue, bool> Condition { get; }

		/// <summary>
		/// Returns true if this rules has a match
		/// </summary>
		public bool Matched => Arg != null;

		/// <summary>
		/// Creates a command argument rule
		/// </summary>
		/// <param name="condition">matching condition</param>
		public CommandArgRule(Func<CommandArgValue, bool> condition)
		{
			//unlinked
			Id = -1;
			//unmatched
			Arg = null;
			//
			Condition = condition;
		}

		/// <summary>
		/// Creates a command argument rule
		/// </summary>
		/// <param name="condition">matching condition</param>
		/// <returns></returns>
		public static CommandArgRule Create(Func<CommandArgValue, bool> condition) => new CommandArgRule(condition);

		/// <summary>
		/// Creates a new command argument main entry key
		/// </summary>
		/// <param name="entries">valid string entries</param>
		/// <returns></returns>
		public static CommandArgRule Command(params string[] entries)
		{
			if (entries == null)
			{
				throw new ArgumentException("command key entries is empty or null");
			}
			return new CommandArgRule((arg) => entries.Contains(arg.Key) && !arg.IsKeyPair);
		}

		/// <summary>
		/// Creates a new command with key matching specific type and non-keypair
		/// </summary>
		/// <param name="type">matching types</param>
		/// <returns></returns>
		public static CommandArgRule KeyTypeAs(CommandArgItemType type)
		{
			if (type == CommandArgItemType.None)
			{
				throw new ArgumentException("invalid rule none type match");
			}
			return new CommandArgRule((arg) => !arg.IsKeyPair && (type & arg.Type) != 0);
		}

		/// <summary>
		/// Creates a new command with key equal a string
		/// </summary>
		/// <param name="key">key string value</param>
		/// <returns></returns>
		public static CommandArgRule KeyValueEquals(string key)
		{
			if (key == null)
			{
				throw new ArgumentException("invalid rule key is null");
			}
			return new CommandArgRule((arg) => !arg.IsKeyPair && arg.Key == key.Trim());
		}

		/// <summary>
		/// Creates a new command (key, pair) equals 
		/// </summary>
		/// <param name="key">key string value</param>
		/// <param name="value">value string</param>
		/// <returns></returns>
		public static CommandArgRule KeyPairEquals(string key, string value)
		{
			if (key == null || value == null)
			{
				throw new ArgumentException("invalid rule key, pair is null");
			}
			return new CommandArgRule((arg) =>
				arg.IsKeyPair &&
				arg.Key == key.Trim() &&
				(arg as CommandArgKeypair).Value == value.Trim());
		}

		/// <summary>
		/// Creates a new command (key, pair) as a key equals an string and an specific value-type
		/// </summary>
		/// <param name="key">key string</param>
		/// <param name="valueType">value-type</param>
		/// <returns></returns>
		public static CommandArgRule KeyPairAs(string key, CommandArgItemType valueType)
		{
			if (key == null || valueType == CommandArgItemType.None)
			{
				throw new ArgumentException("invalid rule key and/or value type");
			}
			return new CommandArgRule((arg) =>
				arg.IsKeyPair &&
				arg.Key == key.Trim() &&
				(arg as CommandArgKeypair).ValueType == valueType
			);
		}

		/// <summary>
		/// Clear all previous matchings if any
		/// </summary>
		public void Clear() => Arg = null;

		/// <summary>
		/// Performs a match if this condition is met
		/// </summary>
		/// <param name="arg">command argument value</param>
		/// <returns></returns>
		public bool Match(CommandArgValue arg)
		{
			if (Matched)
			{
				return true;
			}
			if (arg != null && Condition(arg))
			{
				Arg = arg;
				return true;
			}
			return false;
		}

		public override string ToString() => $"{Arg}";

	}

	/// <summary>
	/// Implements a command argument rule action
	/// </summary>
	public class CommandArgRulesAction
	{
		/// <summary>
		/// Action to perform if all rules are met
		/// </summary>
		public Action Action { get; }

		/// <summary>
		/// List of all rules
		/// </summary>
		public List<CommandArgRule> Rules { get; }

		/// <summary>
		/// Gets the amount of rules without the command entry
		/// </summary>
		public int Count => Rules.Count;

		/// <summary>
		/// Get the command or first rule
		/// </summary>
		public CommandArgRule Command { get; }

		/// <summary>
		/// Gets the name of the action rule
		/// </summary>
		public string Name => Command.Arg.Key;

		/// <summary>
		/// Creates a command argument rule action
		/// </summary>
		/// <param name="command">main command entry</param>
		/// <param name="action">action to execute</param>
		/// <param name="ruleCollection">collection of rules</param>
		public CommandArgRulesAction(CommandArgRule command, Action action,
			IEnumerable<CommandArgRule> ruleCollection = null)
		{
			if ((Command = command) == null)
			{
				throw new ArgumentException("command entry rule cannot be empty or null");
			}
			//link, 0-based
			Command.Id = 0;

			Action = action;
			Rules = (ruleCollection == null) ?
				new List<CommandArgRule>() :
				new List<CommandArgRule>(ruleCollection);
		}

		/// <summary>
		/// Self returning add new rule method
		/// </summary>
		/// <param name="ruleArray">new rule</param>
		/// <returns></returns>
		public CommandArgRulesAction Add(params CommandArgRule[] ruleArray)
		{
			if (ruleArray != null)
			{
				foreach (var rule in ruleArray)
				{
					if (rule == null)
					{
						throw new ArgumentException("cannot add empty rule");
					}
					Rules.Add(rule);
					// 1-based, command has Id = 0
					rule.Id = Count;
				}
			}
			return this;
		}

		/// <summary>
		/// Clear all previous matchings if any
		/// </summary>
		public void Clear()
		{
			Command.Clear();
			Rules.ForEach(rule => rule.Clear());
		}

		/// <summary>
		/// Tries to match all rules against a collection of argument values
		/// </summary>
		/// <param name="argsCollection">collection of argument values</param>
		/// <returns></returns>
		public bool Match(IEnumerable<CommandArgValue> argsCollection)
		{
			if (argsCollection == null)
			{
				return false;
			}
			//must match first
			if (!Command.Match(argsCollection.FirstOrDefault()))
			{
				return false;
			}
			//match other arguments in any position
			foreach (var arg in argsCollection.Skip(1))
			{
				//try to match any no-matched rule with any argument in any position after first command
				if (!Rules.Where(rule => !rule.Matched).Any(rule => rule.Match(arg)))
				{
					return false;
				}
			}
			//here we must have all rules matched
			return Rules.All(rule => rule.Matched);
		}

		public override string ToString() => $"{Name} ({Rules.Count}) rule(s)";

	}

	/// <summary>
	/// Implements a command argument rule container
	/// </summary>
	public class CommandArgRules
	{
		/// <summary>
		/// List of command argument rule actions
		/// </summary>
		public List<CommandArgRulesAction> Actions { get; }

		/// <summary>
		/// Clear all previous matchings if any
		/// </summary>
		public void Clear()
		{
			Actions.ForEach(action => action.Clear());
		}

		/// <summary>
		/// Creates a command argument rule container
		/// </summary>
		/// <param name="actionCollection"></param>
		public CommandArgRules(IEnumerable<CommandArgRulesAction> actionCollection)
		{
			Actions = (actionCollection == null) ?
				new List<CommandArgRulesAction>() :
				new List<CommandArgRulesAction>(actionCollection);
		}
	}

}
