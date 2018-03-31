using System;
using System.Collections.Generic;
using io = System.IO;
using CsvDb.Query;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using System.Reflection;
using System.IO;

namespace CsvDb
{

	public static class Utils
	{
		//https://msdn.microsoft.com/en-us/magazine/mt808499.aspx

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

		public static IEnumerable<DbQueryExpressionItem> Flatten(
			this List<DbQueryExpressionBase> list)
		{
			foreach (var item in list)
			{
				switch (item.Type)
				{
					case DbQueryExpressionItemType.Logical:
						yield return item;
						break;
					case DbQueryExpressionItemType.Expression:
						var expr = item as DbQueryExpression;
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

		public static string ToYesNo(this bool value) => value ? "Yes" : "No";

		public static string IfYes(this bool value, string text) => value ? text : String.Empty;

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

		public static object LoadKey(this DbColumnTypeEnum keyType, io.BinaryReader reader)
		{
			switch (keyType)
			{
				case DbColumnTypeEnum.Byte:
					return reader.ReadByte();
				case DbColumnTypeEnum.Int16:
					return reader.ReadInt16();
				case DbColumnTypeEnum.Int32:
					return reader.ReadInt32();
				case DbColumnTypeEnum.Double:
					return reader.ReadDouble();
				case DbColumnTypeEnum.String:
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
				case nameof(DbColumnTypeEnum.Byte):
					byte valueByte = Convert.ToByte(key); //.ChangeType(key, TypeCode.Byte);
					writer.Write(valueByte);
					break;
				case nameof(DbColumnTypeEnum.Int16):
					var valueInt16 = Convert.ToInt16(key);
					writer.Write(valueInt16);
					break;
				case nameof(DbColumnTypeEnum.Int32):
					var valueInt32 = Convert.ToInt32(key);
					writer.Write(valueInt32);
					break;
				case nameof(DbColumnTypeEnum.Double):
					var valueDouble = Convert.ToDouble(key);
					writer.Write(valueDouble);
					break;
				case nameof(DbColumnTypeEnum.String):
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
			for(var i = 0; i < length; i++)
			{
				chars[i] = (char)((byte)(chars[i]) ^ MASK);
			}
			return new string(chars);
		}

		public static string IfTrue(this bool value, string ifTrue) => value ? ifTrue : "";

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
