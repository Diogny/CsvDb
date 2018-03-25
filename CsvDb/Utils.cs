using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.CSharp;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using System.IO;
using System.Linq;
using io = System.IO;
using static CsvDb.CsvDbQuery;

namespace CsvDb
{

	public static class Utils
	{
		//https://msdn.microsoft.com/en-us/magazine/mt808499.aspx

		public static bool Parse(this CsvDbQueryParser.TokenStruct token,
			out string left, out string right)
		{
			if (token.Token != CsvDbQueryParser.Token.ColumnIdentifier)
			{
				left = right = null;
				return false;
			}
			return Utils.Parse(token.Value, out left, out right);
		}

		public static bool Parse(this string identifier,
			out string left, out string right)
		{
			left = right = null;
			if (String.IsNullOrWhiteSpace(identifier))
			{
				return false;
			}
			var cols = identifier.Split('.', StringSplitOptions.RemoveEmptyEntries);
			if (cols.Length != 2)
			{
				return false;
			}
			left = cols[0];
			right = cols[1];
			return true;
		}

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

		public static IEnumerable<CsvDbQueryWhereItem> Flatten(
			this List<CsvDbQueryExpressionBase> list)
		{
			foreach (var item in list)
			{
				switch (item.Type)
				{
					case CsvDbQueryWhereItemType.Logical:
						yield return item;
						break;
					case CsvDbQueryWhereItemType.Expression:
						var expr = item as CsvDbQueryExpression;
						yield return expr.Left;
						yield return expr.Operator;
						yield return expr.Right;
						break;
				}
			}
		}

		//any of these chars should be wrapped by ""
		//	"	,	line-break
		internal static char[] CvsChars = new char[] { '"', ',', (Char)10, ' ' };

		public static string ToYesNo(this bool value)
		{
			return value ? "Yes" : "No";
		}

		public static string IfYes(this bool value, string text)
		{
			return value ? text : String.Empty;
		}

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

		public static object LoadKey(this CsvDbColumnTypeEnum keyType, io.BinaryReader reader)
		{
			switch (keyType)
			{
				case CsvDbColumnTypeEnum.Byte:
					return reader.ReadByte();
				case CsvDbColumnTypeEnum.Int16:
					return reader.ReadInt16();
				case CsvDbColumnTypeEnum.Int32:
					return reader.ReadInt32();
				case CsvDbColumnTypeEnum.Double:
					return reader.ReadDouble();
				case CsvDbColumnTypeEnum.String:
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
				case nameof(CsvDbColumnTypeEnum.Byte):
					byte valueByte = Convert.ToByte(key); //.ChangeType(key, TypeCode.Byte);
					writer.Write(valueByte);
					break;
				case nameof(CsvDbColumnTypeEnum.Int16):
					var valueInt16 = Convert.ToInt16(key);
					writer.Write(valueInt16);
					break;
				case nameof(CsvDbColumnTypeEnum.Int32):
					var valueInt32 = Convert.ToInt32(key);
					writer.Write(valueInt32);
					break;
				case nameof(CsvDbColumnTypeEnum.Double):
					var valueDouble = Convert.ToDouble(key);
					writer.Write(valueDouble);
					break;
				case nameof(CsvDbColumnTypeEnum.String):
					var valueString = Convert.ToString(key);
					//byte length
					byte length = (byte)valueString.Length;
					writer.Write(length);
					//chars
					writer.Write(valueString.ToCharArray(), 0, length);
					break;
			}
		}

		public static string IfTrue(this bool value, string ifTrue)
		{
			return value ? ifTrue : "";
		}

		/// <summary>
		/// Graps an string with "" if it has a comma inside
		/// </summary>
		/// <param name="text"></param>
		/// <returns></returns>
		public static string ToCsvCol(this string text, CsvDbColumn column)
		{
			//if we don't know the type or not an string, just return text
			if (column == null || text == null || column.Type != "String")
			{
				return text == null ? "" : text;
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

		public static string CsvStringValue(this string text, CsvDbColumn column)
		{
			if (column == null || text == null || column.Type != "String")
			{
				return text == null ? "" : text;
			}
			var length = text.Length;
			return (text[0] == '"' && text[--length] == '"') ? text.Substring(1, --length) : text;
		}

		public static void CreateClass(CsvDbTable table)
		{
			var sb = new StringBuilder();
			sb.AppendLine("using System;");
			sb.AppendLine();
			sb.AppendLine("namespace CsvDbLib.Compiled");
			sb.AppendLine("{");
			sb.AppendLine($"\t public class {table.Name}");
			sb.AppendLine("\t{");
			//properties
			foreach (var col in table.Columns)
			{
				sb.AppendLine($"\t\tpublic {col.Key} {col.Name} {{get; set; }}");
			}
			sb.AppendLine("\t{");
			sb.AppendLine("}");

			SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(sb.ToString());

			string assemblyName = Path.GetRandomFileName();
			MetadataReference[] references = new MetadataReference[]
			{
				MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
				MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location)
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
				}
				else
				{
					ms.Seek(0, SeekOrigin.Begin);
					Assembly assembly = Assembly.Load(ms.ToArray());

					Type type = assembly.GetType("RoslynCompileSample.Writer");
					object obj = Activator.CreateInstance(type);
					type.InvokeMember("Write",
							BindingFlags.Default | BindingFlags.InvokeMethod,
							null,
							obj,
							new object[] { "Hello World" });
				}
			}

		}
	}
}
