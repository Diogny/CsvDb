using System;
using System.Collections.Generic;
using io = System.IO;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using System.Reflection;
using System.IO;

namespace CsvDb
{
	/// <summary>
	/// static utils class
	/// </summary>
	public static class Utils
	{
		//https://msdn.microsoft.com/en-us/magazine/mt808499.aspx

		/// <summary>
		/// numeric mask
		/// </summary>
		internal static DbColumnType NumericMask =
			DbColumnType.Byte |
			DbColumnType.Int16 | DbColumnType.Int32 | DbColumnType.Int64 |
			DbColumnType.Float | DbColumnType.Double | DbColumnType.Decimal;

		/// <summary>
		/// returns true if a database column type is numeric
		/// </summary>
		/// <param name="type">database column type</param>
		/// <returns></returns>
		public static bool IsNumeric(this DbColumnType type)
		{
			return (type & NumericMask) != 0;
		}

		/// <summary>
		/// normalize or returns the conversion type for a type check comparison
		/// </summary>
		/// <param name="type">type</param>
		/// <param name="other">other to compare</param>
		/// <returns>needed type for successful comparison, otherwise None</returns>
		public static DbColumnType Normalize(this DbColumnType type, DbColumnType other)
		{
			//if equals, return same
			if (type == other)
			{
				return type;
			}
			//otherwise atomize
			switch (type)
			{
				case DbColumnType.Byte:
				case DbColumnType.Int16:
				case DbColumnType.Int32:
				case DbColumnType.Int64:
					//cast to Int64 if differents
					return (other == DbColumnType.Byte ||
							other == DbColumnType.Int16 ||
							other == DbColumnType.Int32 ||
							other == DbColumnType.Int64) ? DbColumnType.Int64 : DbColumnType.None;
				case DbColumnType.Float:
				case DbColumnType.Double:
				case DbColumnType.Decimal:
					//cast to decimal if different
					return (other == DbColumnType.Float ||
						other == DbColumnType.Double ||
						other == DbColumnType.Decimal) ? DbColumnType.Decimal : DbColumnType.None;
			}
			// if DbColumnType.String, DbColumnType.Char drives here then none for these two
			return DbColumnType.None;
		}

		/// <summary>
		/// returns true if two database column types can be safely compared
		/// </summary>
		/// <param name="type">type</param>
		/// <param name="other">other to compare</param>
		/// <returns></returns>
		public static bool CanCompareTo(this DbColumnType type, DbColumnType other)
		{
			return type.Normalize(other) != DbColumnType.None;
		}

		/// <summary>
		/// returns a text with the difference of two times
		/// </summary>
		/// <param name="span"></param>
		/// <returns></returns>
		public static string Difference(this TimeSpan span)
		{
			if (span.Seconds == 0)
			{
				return $"{span.Milliseconds} ms";
			}
			else if (span.Minutes == 0)
			{
				return $"{span.Seconds}.{span.Milliseconds} sec";
			}
			else if (span.Hours == 0)
			{
				return $"{span.Minutes}:{span.Seconds}.{span.Milliseconds} min";
			}
			else
			{
				return span.ToString("hh\\:mm\\:ss");
			}
		}

		//public static IEnumerable<DbQueryExpressionItem> Flatten(
		//	this List<DbQueryExpressionBase> list)
		//{
		//	foreach (var item in list)
		//	{
		//		switch (item.Type)
		//		{
		//			case DbQueryExpressionItemType.Logical:
		//				yield return item;
		//				break;
		//			case DbQueryExpressionItemType.Expression:
		//				var expr = item as DbQueryExpression;
		//				yield return expr.Left;
		//				yield return expr.Operator;
		//				yield return expr.Right;
		//				break;
		//		}
		//	}
		//}

		/// <summary>
		/// removes wrapping single quotes from an string text
		/// </summary>
		/// <param name="text">string text</param>
		/// <returns>unwrapped string text</returns>
		public static string UnwrapQuotes(this string text)
		{
			if (text != null && text[0] == '\'' && text[text.Length - 1] == '\'')
			{
				return text.Substring(1, text.Length - 2);
			}
			return text;
		}

		/// <summary>
		/// converts a numeric text into its object number
		/// </summary>
		/// <param name="text">text</param>
		/// <returns></returns>
		public static object ToNumber(this string text)
		{
			var tuple = text.ToNumberType();
			return tuple?.Item2;
		}

		/// <summary>
		/// tries to convert a numeric text into its numeric object and type
		/// </summary>
		/// <param name="text">numeric text</param>
		/// <returns></returns>
		public static Tuple<DbColumnType, object> ToNumberType(this string text)
		{
			if (Int32.TryParse(text, out Int32 int32Value))
			{
				return new Tuple<DbColumnType, object>(DbColumnType.Int32, int32Value);
			}
			else if (Double.TryParse(text, out double doubleValue))
			{
				return new Tuple<DbColumnType, object>(DbColumnType.Double, doubleValue);
			}
			else if (Decimal.TryParse(text, out Decimal decimalValue))
			{
				return new Tuple<DbColumnType, object>(DbColumnType.Decimal, decimalValue);
			}
			return null;
		}

		/// <summary>
		/// tries to convert a token type into its database column type
		/// </summary>
		/// <param name="item">token type</param>
		/// <returns></returns>
		public static DbColumnType ToCast(this TokenType item)
		{
			var itemValue = (int)item;
			//base 
			var tokenStartValue = (int)TokenType.Byte;
			//max 
			var tokenEndValue = (int)TokenType.Int64;

			return (itemValue >= tokenStartValue && itemValue <= tokenEndValue) ?
				(DbColumnType)(itemValue - tokenStartValue + 1) :
				DbColumnType.None;
		}


		//any of these chars should be wrapped by ""
		//	"	,	line-break
		internal static char[] CvsChars = new char[] { '"', ',', (Char)10, ' ' };

		public static string ToYesNo(this bool value) => value ? "Yes" : "No";

		public static string IfTrue(this bool value, string text) => value ? text : String.Empty;

		public static IEnumerable<K> GetAllKeysAs<T, K>(
			this IEnumerable<KeyValuePair<T, int>> collection)
		{
			if (collection == null)
			{
				yield break;
			}
			else
			{
				var type = typeof(K);
				foreach (var pair in collection)
				{
					var value = (K)Convert.ChangeType(pair.Key, type);
					yield return value;
				}
			}
		}

		public static IEnumerable<string> GetAllKeyStrings<T>(
			this IEnumerable<KeyValuePair<T, List<int>>> collection)
		{
			if (collection == null)
			{
				yield break;
			}
			else
			{
				foreach (var pair in collection)
				{
					var str = Convert.ToString(pair.Key);
					yield return str;
				}
			}
		}

		public static object LoadKey(this DbColumnType keyType, io.BinaryReader reader)
		{
			switch (keyType)
			{
				case DbColumnType.Char:
					//
					return reader.ReadChar();
				case DbColumnType.Byte:
					//
					return reader.ReadByte();
				case DbColumnType.Int16:
					//
					return reader.ReadInt16();
				case DbColumnType.Int32:
					//
					return reader.ReadInt32();
				case DbColumnType.Int64:
					//
					return reader.ReadInt64();
				case DbColumnType.Float:
					//
					return reader.ReadSingle();
				case DbColumnType.Double:
					//
					return reader.ReadDouble();
				case DbColumnType.Decimal:
					//
					return reader.ReadDecimal();
				case DbColumnType.String:
					//
					var length = reader.ReadByte();
					var chars = reader.ReadChars(length);
					return new string(chars);
				default:
					return null;
			}
		}

		public static void StoreKey<T>(this T key, io.BinaryWriter writer)
		{
			var typeName = key.GetType().Name;
			switch (typeName)
			{
				case nameof(DbColumnType.Char):
					char valueChar = Convert.ToChar(key);
					writer.Write(valueChar);
					break;
				case nameof(DbColumnType.Byte):
					byte valueByte = Convert.ToByte(key); //.ChangeType(key, TypeCode.Byte);
					writer.Write(valueByte);
					break;
				case nameof(DbColumnType.Int16):
					var valueInt16 = Convert.ToInt16(key);
					writer.Write(valueInt16);
					break;
				case nameof(DbColumnType.Int32):
					var valueInt32 = Convert.ToInt32(key);
					writer.Write(valueInt32);
					break;
				case nameof(DbColumnType.Int64):
					var valueInt64 = Convert.ToInt64(key);
					writer.Write(valueInt64);
					break;
				case nameof(DbColumnType.Float):
					var valueFloat = Convert.ToSingle(key);
					writer.Write(valueFloat);
					break;
				case nameof(DbColumnType.Double):
					var valueDouble = Convert.ToDouble(key);
					writer.Write(valueDouble);
					break;
				case nameof(DbColumnType.Decimal):
					var valueDecimal = Convert.ToDecimal(key);
					writer.Write(valueDecimal);
					break;
				case nameof(DbColumnType.String):
					var valueString = Convert.ToString(key);
					//byte length
					byte length = (byte)valueString.Length;
					writer.Write(length);
					//chars
					writer.Write(valueString.ToCharArray(), 0, length);
					break;
			}
		}

		static byte MASK = 0b1101011;

		public static void BinarySave(this string text, io.BinaryWriter writer)
		{
			if (String.IsNullOrEmpty(text))
			{
				text = String.Empty;
			}
			byte length = (byte)text.Length;
			writer.Write(length);
			var chars = text.ToCharArray();

			//simple encode
			for (var i = 0; i < length; i++)
			{
				chars[i] = (char)((byte)(chars[i]) ^ MASK);
			}
			writer.Write(chars, 0, length);
		}

		public static string BinaryRead(this io.BinaryReader reader)
		{
			byte length = reader.ReadByte();
			var chars = new char[length];
			var readed = reader.Read(chars, 0, length);
			if (readed != length)
			{
				throw new ArgumentException("corrupted string reading");
			}
			//simple decode
			for (var i = 0; i < length; i++)
			{
				chars[i] = (char)((byte)(chars[i]) ^ MASK);
			}
			return new string(chars);
		}

		/// <summary>
		/// Graps an string with "" if it has a comma inside
		/// </summary>
		/// <param name="text"></param>
		/// <returns></returns>
		public static string ToCsvCol(this string text, DbColumn column)
		{
			//if we don't know the type or not an string, just return text
			if (column == null || text == null || column.Type != "String")
			{
				return text ?? "";
			}

			var ndx = text.IndexOfAny(CvsChars);
			if (ndx == 0)
			{
				//any double-quote char " must be doubled: ""
				text = text.Replace("\"", "\"\"");
			}
			// double-quotes (") comma (,) line-breaks (10) or column:String should be doubled quoted ""
			return (ndx >= 0) ? // || column.Type == "String") ?
				$"\"{text}\"" :
				text;
			//text.IndexOf(',') < 0 ? text : $"\"{text}\"";
		}

		public static string CsvStringValue(this string text, DbColumn column)
		{
			if (column == null || text == null || column.Type != "String")
			{
				return text ?? "";
			}
			var length = text.Length;
			return (text[0] == '"' && text[--length] == '"') ? text.Substring(1, --length) : text;
		}

		//public static CompilerResults CreateType(string name, IDictionary<string, Type> props)
		//{
		//	var csc = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } });

		//	var parameters = new CompilerParameters(new[] {
		//		"mscorlib.dll",
		//		"System.Core.dll"
		//	}, "CsvDb.Dynamic.dll", false);
		//	parameters.GenerateExecutable = false;

		//	var compileUnit = new CodeCompileUnit();
		//	var ns = new CodeNamespace("CsvDb.Dynamic");
		//	compileUnit.Namespaces.Add(ns);
		//	ns.Imports.Add(new CodeNamespaceImport("System"));

		//	var classType = new CodeTypeDeclaration(name);
		//	classType.Attributes = MemberAttributes.Public;
		//	ns.Types.Add(classType);

		//	foreach (var prop in props)
		//	{
		//		var fieldName = "_" + prop.Key;
		//		var field = new CodeMemberField(prop.Value, fieldName);
		//		classType.Members.Add(field);

		//		var property = new CodeMemberProperty();
		//		property.Attributes = MemberAttributes.Public | MemberAttributes.Final;
		//		property.Type = new CodeTypeReference(prop.Value);
		//		property.Name = prop.Key;
		//		property.GetStatements.Add(
		//			new CodeMethodReturnStatement(
		//				new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), fieldName)));
		//		property.SetStatements.Add(
		//			new CodeAssignStatement(new CodeFieldReferenceExpression(
		//				new CodeThisReferenceExpression(), fieldName), new CodePropertySetValueReferenceExpression()));
		//		classType.Members.Add(property);
		//	}

		//	var results = csc.CompileAssemblyFromDom(parameters, compileUnit);
		//	results.Errors.Cast<CompilerError>().ToList().ForEach(error => Console.WriteLine(error.ErrorText));

		//	return results;
		//}

		/// <summary>
		/// testings
		/// </summary>
		/// <param name="db"></param>
		/// <returns></returns>
		public static Assembly CreateDbClasses(CsvDb db)
		{
			try
			{
				var sb = new StringBuilder();
				sb.AppendLine("using System;");
				sb.AppendLine("using System.Collections.Generic;");
				sb.AppendLine("using CsvDb;");
				sb.AppendLine();
				sb.AppendLine("namespace CsvDb.Dynamic");
				sb.AppendLine("{");
				//interface
				sb.AppendLine(@"	public interface IDbColumnClass {
		bool Unique { get; }
		IEnumerable<KeyValuePair<string,int>> Keys { get; }
		DbTable Table { get; }
	}
");

				foreach (var table in db.Tables)
				{
					sb.AppendLine($"	public class {table.Name}: IDbColumnClass");
					sb.AppendLine("	{");
					//column properties
					foreach (var col in table.Columns)
					{
						sb.AppendLine($"		public {col.Type} {col.Name} {{get; set; }}");
					}
					//custom properties
					var keys = table.Columns.Where(col => col.Key).ToList();

					var oneKey = keys.Count == 1;
					sb.AppendLine($"		public bool Unique => {oneKey.ToString().ToLower()};");

					var keyStr = String.Join(", ", keys.Select(col => $"new KeyValuePair<string,int>(\"{col.Name}\", {col.Index} )"));
					sb.AppendLine(@"		public IEnumerable<KeyValuePair<string,int>> Keys
		{
			get
			{
				return new KeyValuePair<string,int>[] { " + keyStr + @" };
			}
		}");
					sb.AppendLine("		public DbTable Table { get; private set; }");
					//link method
					sb.AppendLine("		public void Link(DbTable table)" +
						@"
		{
			Table = table;
		}");
					sb.AppendLine("	}");
					sb.AppendLine("");
				}

				sb.AppendLine("}");

				Console.WriteLine($"class(es) to be generated:\r\n{sb.ToString()}");

				SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(sb.ToString());

				string assemblyName = Path.GetRandomFileName();
				var assemblyPath = Path.GetDirectoryName(typeof(object).Assembly.Location);

				MetadataReference[] references = new MetadataReference[]
				{
					//MetadataReference.CreateFromFile(Path.Combine(assemblyPath, "mscorlib.dll")),
					MetadataReference.CreateFromFile(Path.Combine(assemblyPath, "System.Runtime.dll")),
					MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
					MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
				//MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
					MetadataReference.CreateFromFile(typeof(CsvDb).Assembly.Location) // Path.Combine( Environment.CurrentDirectory,"CsvDb.dll"))
				};

				CSharpCompilation compilation = CSharpCompilation.Create(
						assemblyName,
						syntaxTrees: new[] { syntaxTree },
						references: references,
						options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

				using (var ms = new MemoryStream())
				{
					EmitResult result = compilation.Emit(ms);

					if (!result.Success)
					{
						IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
								diagnostic.IsWarningAsError ||
								diagnostic.Severity == DiagnosticSeverity.Error);

						foreach (Diagnostic diagnostic in failures)
						{
							Console.Error.WriteLine("{0}: {1}", diagnostic.Id, diagnostic.GetMessage());
						}
						return null;
					}
					else
					{
						ms.Seek(0, SeekOrigin.Begin);
						Assembly assembly = Assembly.Load(ms.ToArray());

						//Type type = assembly.GetType("RoslynCompileSample.Writer");
						//object obj = Activator.CreateInstance(type);
						//type.InvokeMember("Write",
						//		BindingFlags.Default | BindingFlags.InvokeMethod,
						//		null,
						//		obj,
						//		new object[] { "Hello World" });

						return assembly;
					}
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine($"class compile error:\r\n{ex.Message}");
				return null;
			}
		}
	}
}
