using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	//https://github.com/kahanu/System.Linq.Dynamic/blob/master/Src/System.Linq.Dynamic/DynamicLinq.cs

	public class DbClassGenerator
	{
		/// <summary>
		/// Database
		/// </summary>
		public CsvDb Database { get; }

		/// <summary>
		/// True if generated
		/// </summary>
		public bool Generated { get; }

		/// <summary>
		/// path to generated dll
		/// </summary>
		public string Path { get; }

		/// <summary>
		/// assembly name
		/// </summary>
		public string AssemblyName { get; }

		private Dictionary<string, DbClass> tableTypes;

		/// <summary>
		/// indexer to get table types
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <returns></returns>
		public DbClass this[string tableName] => tableTypes.TryGetValue(tableName, out DbClass type) ? type : null;

		public DbClassGenerator(CsvDb db)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("cannot generate database class(es) without a database");
			}
			Generated = io.File.Exists(Path = $"{db.BinaryPath}{(AssemblyName = $"{db.Name}.dll")}");

			tableTypes = new Dictionary<string, DbClass>();
			if (!Compile())
			{
				throw new ArgumentException($"cnnot compile database: {db.Name}");
			}
		}

		//https://stackoverflow.com/questions/41784393/how-to-emit-a-type-in-net-core?noredirect=1&lq=1

		private static void CreateApply(TypeBuilder tb)
		{
			// Add 'MyMethod' method to the class, with the specified attribute and signature.
			MethodBuilder myMethod = tb.DefineMethod("Apply",
				 MethodAttributes.Public,
				 CallingConventions.Standard,
				 typeof(bool),
				 new Type[] { typeof(DbProperty[]), typeof(object[]) }
			);

			ILGenerator il = myMethod.GetILGenerator();
			/*
			public bool Apply(Metadata[] props, object[] parameters)
			{
				if (props == null || parameters == null || props.Length != parameters.Length)
				{
					return false;
				}
				for (var i = 0; i < parameters.Length; i++)
				{
					props[i].Prop.SetValue(this, parameters[i]);
				}
				return true;
			}
			 */

			var dbPropPropProperty = typeof(DbProperty).GetProperty("Prop").GetMethod;
			var propInfoSetValue = typeof(PropertyInfo).GetMethod("SetValue",
				new Type[] { typeof(object), typeof(object) });

			//locals
			var V0 = il.DeclareLocal(typeof(bool));         // 0
			var V1 = il.DeclareLocal(typeof(bool));         // 1
			var V2 = il.DeclareLocal(typeof(int));					// 2
			var V3 = il.DeclareLocal(typeof(bool));         // 3

			var IL_0014 = il.DefineLabel();
			var IL_0015 = il.DefineLabel();
			var IL_001e = il.DefineLabel();
			var IL_0022 = il.DefineLabel();
			var IL_0048 = il.DefineLabel();
			var IL_003a = il.DefineLabel();

			//if props == null
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Brfalse_S, IL_0014);

			//if parameters == null
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Brfalse_S, IL_0014);

			//get props.Length
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldlen);
			il.Emit(OpCodes.Conv_I4);

			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldlen);
			il.Emit(OpCodes.Conv_I4);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Br_S, IL_0015);

			il.MarkLabel(IL_0014);
			il.Emit(OpCodes.Ldc_I4_1);

			il.MarkLabel(IL_0015);
			il.Emit(OpCodes.Stloc, V0);  //stloc.0
			il.Emit(OpCodes.Ldloc, V0);
			il.Emit(OpCodes.Brfalse_S, IL_001e);

			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Stloc, V1);  //stloc.1
			il.Emit(OpCodes.Br_S, IL_0048);

			il.MarkLabel(IL_001e);
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Stloc, V2);  //stloc.2
			il.Emit(OpCodes.Br_S, IL_003a);

			il.MarkLabel(IL_0022);
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldloc, V2);
			il.Emit(OpCodes.Ldelem_Ref);
			il.Emit(OpCodes.Callvirt, dbPropPropProperty);

			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldloc, V2);
			il.Emit(OpCodes.Ldelem_Ref);
			il.Emit(OpCodes.Callvirt, propInfoSetValue);

			il.Emit(OpCodes.Ldloc, V2);
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Stloc, V2);  //stloc.2

			il.MarkLabel(IL_003a);
			il.Emit(OpCodes.Ldloc, V2);
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldlen);
			il.Emit(OpCodes.Conv_I4);
			il.Emit(OpCodes.Clt);
			il.Emit(OpCodes.Stloc, V3);  //stloc.3
			il.Emit(OpCodes.Ldloc, V3);
			il.Emit(OpCodes.Brtrue_S, IL_0022);

			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Stloc, V1);  //stloc.1

			il.MarkLabel(IL_0048);
			il.Emit(OpCodes.Ldloc, V1);

			il.Emit(OpCodes.Ret);
		}

		private static void CreateToString(TypeBuilder tb)
		{
			MethodBuilder myMethod = tb.DefineMethod("ToString",
				 MethodAttributes.Public,
				 CallingConventions.Standard,
				 typeof(string),
				 new Type[] { typeof(DbProperty[]) }
			);

			ILGenerator il = myMethod.GetILGenerator();

			MethodInfo stringJoinMethod = typeof(String).GetMethod("Join",
				new Type[] { typeof(string), typeof(string[]) });

			var dbPropTypeProperty = typeof(DbProperty).GetProperty("Type").GetMethod;
			var dbPropPropProperty = typeof(DbProperty).GetProperty("Prop").GetMethod;

			var propInfoGetValue = typeof(PropertyInfo).GetMethod("GetValue",
				new Type[] { typeof(object) });

			var utilsGetStr = typeof(Utils).GetMethod("GetStr");

			//locals
			var length = il.DeclareLocal(typeof(int));         // 0
			var columns = il.DeclareLocal(typeof(string[]));   // 1
			var i = il.DeclareLocal(typeof(int));              // 2
			var prop = il.DeclareLocal(typeof(DbProperty));    // 3
			var cond = il.DeclareLocal(typeof(bool));          // 4
			var result = il.DeclareLocal(typeof(string));      // 5

			//labels
			var propLengthNull = il.DefineLabel();
			var setPropLength = il.DefineLabel();
			var loopBody = il.DefineLabel();
			var loopCondition = il.DefineLabel();

			/*
			public string ToString(Metadata[] props)
			{
				var length = (props == null) ? 0 : props.Length;
				var columns = new object[length];
				for (var i = 0; i < length; i++)
				{
					var prop = props[i];
					columns[i] = prop.Type.GetStr(prop.Prop.GetValue(this));
				}
				return String.Join(", ", columns);
			}
			*/

			il.Emit(OpCodes.Ldarg_1);
			//getIl.Emit(OpCodes.Call, typeof(Console).GetMethod("Write", new Type[] { typeof(string) }));

			//if props == NULL
			il.Emit(OpCodes.Brfalse_S, propLengthNull);

			//props.Length
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldlen);
			il.Emit(OpCodes.Conv_I4);

			//goto get props.Length
			il.Emit(OpCodes.Br_S, setPropLength);

			il.MarkLabel(propLengthNull);
			////length = 0
			il.Emit(OpCodes.Ldc_I4_0);

			il.MarkLabel(setPropLength);
			il.Emit(OpCodes.Stloc, length);  //stloc.0
			il.Emit(OpCodes.Ldloc, length);
			//create new object[length]
			il.Emit(OpCodes.Newarr, typeof(object));
			il.Emit(OpCodes.Stloc, columns); //stloc.1

			// var i = 0;
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Stloc, i);  //stloc.2
			il.Emit(OpCodes.Br_S, loopCondition);

			//body loop
			il.MarkLabel(loopBody);
			//var prop = props[i];
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldloc, i);
			il.Emit(OpCodes.Ldelem_Ref);
			il.Emit(OpCodes.Stloc, prop);

			//
			il.Emit(OpCodes.Ldloc, columns);
			il.Emit(OpCodes.Ldloc, i);
			//get prop.Type
			il.Emit(OpCodes.Ldloc, prop);
			il.Emit(OpCodes.Callvirt, dbPropTypeProperty);

			//getIl.Emit(OpCodes.Pop);

			//get prop.Prop
			il.Emit(OpCodes.Ldloc, prop);
			il.Emit(OpCodes.Callvirt, dbPropPropProperty);

			//getIl.Emit(OpCodes.Pop);

			//get prop.Prop.GetValue()
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Callvirt, propInfoGetValue);

			il.Emit(OpCodes.Call, utilsGetStr);

			il.Emit(OpCodes.Stelem_Ref);

			// i++
			il.Emit(OpCodes.Ldloc, i);
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Stloc, i);

			il.MarkLabel(loopCondition);
			//i < length
			il.Emit(OpCodes.Ldloc, i);
			il.Emit(OpCodes.Ldloc, length);
			il.Emit(OpCodes.Clt);
			il.Emit(OpCodes.Stloc_S, cond);
			il.Emit(OpCodes.Ldloc_S, cond);
			il.Emit(OpCodes.Brtrue_S, loopBody);

			//string join
			il.Emit(OpCodes.Ldstr, ", ");
			il.Emit(OpCodes.Ldloc, columns);

			il.Emit(OpCodes.Call, stringJoinMethod);
			il.Emit(OpCodes.Stloc_S, result);
			il.Emit(OpCodes.Ldloc_S, result);

			//getIl.Emit(OpCodes.Ldstr, ", ");

			il.Emit(OpCodes.Ret);
		}

		private static void CreateProperty(TypeBuilder tb, string propertyName, Type propertyType)
		{
			FieldBuilder fieldBuilder = tb.DefineField(
				"_" + propertyName, propertyType, FieldAttributes.Private);

			PropertyBuilder propertyBuilder =
				tb.DefineProperty(propertyName, PropertyAttributes.HasDefault, propertyType, null);

			MethodBuilder getPropMthdBldr =
				tb.DefineMethod("get_" + propertyName,
					MethodAttributes.Public |
						MethodAttributes.SpecialName |
						MethodAttributes.HideBySig,
					propertyType,
					Type.EmptyTypes
			);
			ILGenerator getIl = getPropMthdBldr.GetILGenerator();

			getIl.Emit(OpCodes.Ldarg_0);
			getIl.Emit(OpCodes.Ldfld, fieldBuilder);
			getIl.Emit(OpCodes.Ret);

			MethodBuilder setPropMthdBldr =
				tb.DefineMethod("set_" + propertyName,
					MethodAttributes.Public |
						MethodAttributes.SpecialName |
						MethodAttributes.HideBySig,
					null,
					new[] { propertyType }
			);

			ILGenerator setIl = setPropMthdBldr.GetILGenerator();
			Label modifyProperty = setIl.DefineLabel();
			Label exitSet = setIl.DefineLabel();

			setIl.MarkLabel(modifyProperty);
			setIl.Emit(OpCodes.Ldarg_0);
			setIl.Emit(OpCodes.Ldarg_1);
			setIl.Emit(OpCodes.Stfld, fieldBuilder);

			setIl.Emit(OpCodes.Nop);
			setIl.MarkLabel(exitSet);
			setIl.Emit(OpCodes.Ret);

			propertyBuilder.SetGetMethod(getPropMthdBldr);
			propertyBuilder.SetSetMethod(setPropMthdBldr);
		}

		//define a method like Apply to assign values to all properties
		//  values is object[] with null if skip

		private TypeBuilder GetTypeBuilder(ModuleBuilder moduleBuilder, string typeSignature)
		{
			TypeBuilder tb = moduleBuilder.DefineType(typeSignature,
							TypeAttributes.Public |
							TypeAttributes.Class |
							TypeAttributes.AutoClass |
							TypeAttributes.AnsiClass |
							TypeAttributes.BeforeFieldInit |
							TypeAttributes.AutoLayout,
							null);
			return tb;
		}

		private TypeInfo CreateTypeFromTable(ModuleBuilder moduleBuilder, DbTable table)
		{
			TypeBuilder tb = GetTypeBuilder(moduleBuilder, table.Name);

			ConstructorBuilder constructor = tb.DefineDefaultConstructor(
				MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);

			foreach (var prop in table.Columns)
			{
				CreateProperty(tb, prop.Name, Type.GetType($"System.{prop.Type}"));
			}
			//create Apply(object[] values) method
			CreateApply(tb);

			//create ToString(Metadata[] meta) method
			CreateToString(tb);

			TypeInfo objectTypeInfo = tb.CreateTypeInfo();
			return objectTypeInfo;
		}

		/// <summary>
		/// Compile database table(s) into an assembly with class(es)
		/// </summary>
		/// <returns></returns>
		private bool Compile()
		{
			try
			{
				var an = new AssemblyName(AssemblyName);

				var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
					new AssemblyName(Guid.NewGuid().ToString()),
					AssemblyBuilderAccess.Run);

				//it's fast
				//but lack of AssemblyBuilderAccess.RunAndSave can't save to disk

				ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("MainModule");

				foreach (var table in Database.Tables)
				{
					var typeInfo = CreateTypeFromTable(moduleBuilder, table);
					var type = typeInfo.AsType();

					//generate property info data
					var propInfoCollection = new List<DbProperty>();
					foreach (var col in table.Columns)
					{
						var prop = type.GetProperty(col.Name);
						if (prop == null)
						{
							throw new ArgumentException($"cannot generate: { col } metadata");
						}
						propInfoCollection.Add(new DbProperty(prop, col.TypeEnum));
					}
					tableTypes.Add(table.Name, new DbClass(type, propInfoCollection.ToArray()));
				}

				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"  error: {ex.Message}");
				return false;
			}
		}
	}

	public class DbClass
	{
		/// <summary>
		/// Type of the database class
		/// </summary>
		public Type Type { get; }

		/// <summary>
		/// Property metadata info
		/// </summary>
		public DbProperty[] Props { get; }

		internal DbClass(Type type, DbProperty[] props)
		{
			Type = type;
			Props = props;
		}
		public override string ToString() => $"{Type} ({Props.Length})";
	}

	public class DbProperty
	{
		/// <summary>
		/// PropertryInfo
		/// </summary>
		public PropertyInfo Prop { get; }

		/// <summary>
		/// Database column type
		/// </summary>
		public DbColumnType Type { get; }

		internal DbProperty(PropertyInfo prop, DbColumnType type)
		{
			Prop = prop;
			Type = type;
		}
		public override string ToString() => $"({Type}) {Prop}";
	}
}
