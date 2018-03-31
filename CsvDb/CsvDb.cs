using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	[Flags]
	public enum DbOption
	{
		/// <summary>
		/// Creates a new empty database if no __tables.json provided
		/// </summary>
		Create,
		/// <summary>
		/// Opens for reading only
		/// </summary>
		Open,
		/// <summary>
		/// Opens for editing
		/// </summary>
		OpenEdit
	}

	public enum DbIndexItemsPolicy
	{
		ReadAlways,
		ReadAndStoreAlways,
		ReadKeepMostFrequentAlways
	}

	internal class DbSchemaConfig
	{
		//binary structure of database, test speed

		public String Version { get; set; }

		//Major.Minor.Build.Revision	3.5.234.02
		public System.Version VersionInfo => new System.Version(Version);

		public String Description { get; set; }

		public String Name { get; set; }

		public int PageSize { get; set; }

		public List<DbTable> Tables { get; set; }

		public DbSchemaConfig() { }

		public static DbSchemaConfig Load(io.BinaryReader reader)
		{
			var obj = new DbSchemaConfig()
			{
				Version = reader.BinaryRead(),
				Description = reader.BinaryRead(),
				Name = reader.BinaryRead(),

				PageSize = reader.ReadInt32(),
				Tables = new List<DbTable>()
			};
			//tables
			var pageCount = reader.ReadInt32();
			for (var t = 0; t < pageCount; t++)
			{
				var table = new DbTable()
				{
					Name = reader.BinaryRead(),
					FileName = reader.BinaryRead(),
					Generate = reader.ReadBoolean(),
					Multikey = reader.ReadBoolean(),
					Rows = reader.ReadInt32(),
					Pager = DbTablePager.Load(reader),
					Count = reader.ReadInt32(),
					Columns = new List<DbColumn>()
				};
				//read columns
				for (var c = 0; c < table.Count; c++)
				{
					var col = new DbColumn()
					{
						Indexer = reader.BinaryRead(),
						Unique = reader.ReadBoolean(),
						Name = reader.BinaryRead(),
						Index = reader.ReadInt32(),
						Type = reader.BinaryRead(),
						Key = reader.ReadBoolean(),
						Indexed = reader.ReadBoolean()
					};
					table.Columns.Add(col);
				}
				obj.Tables.Add(table);
			}
			return obj;
		}

		public void Save(io.BinaryWriter writer, bool saveJson = false)
		{
			Version.BinarySave(writer);
			Description.BinarySave(writer);
			Name.BinarySave(writer);

			//page size
			writer.Write(PageSize);

			//tables
			int pageCount = Tables.Count;
			writer.Write(pageCount);

			foreach (var table in Tables)
			{
				table.Name.BinarySave(writer);
				table.FileName.BinarySave(writer);
				writer.Write(table.Generate);
				writer.Write(table.Multikey);
				//
				writer.Write(table.Rows);
				//pager
				table.Pager.Store(writer);
				//columns
				writer.Write(table.Count);

				foreach (var column in table.Columns)
				{
					column.Indexer.BinarySave(writer);
					writer.Write(column.Unique);
					column.Name.BinarySave(writer);
					writer.Write(column.Index);
					column.Type.BinarySave(writer);
					writer.Write(column.Key);
					writer.Write(column.Indexed);
				}
			}
		}

	}

	public class CsvDb : IDisposable
	{
		public string LogPath { get; protected internal set; }

		public string BinaryPath { get; protected internal set; }

		public string Name { get { return Schema.Name; } }

		public int PageSize { get { return Schema.PageSize; } }

		public DbIndexItemsPolicy ReadPolicy { get; set; }

		internal DbSchemaConfig Schema;

		/// <summary>
		/// Reference to tables
		/// </summary>
		public List<DbTable> Tables
		{
			get { return Schema.Tables; }
			set { Schema.Tables = value; }
		}

		public bool Modified { get; set; }

		public static String SchemaSystemName => "__schema";

		public static String SchemaSystemExtension => "sys";

		public static String SchemaJsonName => "__tables";

		public static String SchemaJsonExtension => "json";

		public static String SchemaSystemFilename => $"{SchemaSystemName}.{SchemaSystemExtension}";

		public static String SchemaJsonFilename => $"{SchemaJsonName}.{SchemaJsonExtension}";

		protected internal String SchemaFilePath = String.Empty;

		protected internal String SchemaJsonFilePath = String.Empty;

		/// <summary>
		/// Creates a new CSV database
		/// </summary>
		/// <param name="path">path to the root of the databse</param>
		/// <param name="dif">for testings only, will be removed</param>
		public CsvDb(string path)
		{

			// path\bin\
			//		__schema.sys		// => old  __tables.json
			//		[table].csv
			//		[table].pager
			//		[table].[index].index
			//		[table].[index].index.bin
			//
			// path\
			//		--all files here can be safely deleted, except bin folder and logs is needed
			//		[table].[index].index.txt
			//		[table].[index].index.tree.txt
			//		[table].[index].index.duplicates.txt
			//
			//		[table].log

			//point path to path\bin\
			BinaryPath = io.Path.Combine(LogPath = path, "bin\\");

			//read 
			SchemaFilePath = io.Path.Combine(BinaryPath, SchemaSystemFilename);
			SchemaJsonFilePath = io.Path.Combine(BinaryPath, SchemaJsonFilename);

			if (!io.File.Exists(SchemaFilePath))
			{
				throw new ArgumentException($"cannot load database structure on: {SchemaFilePath}");
			}

			ReadPolicy = DbIndexItemsPolicy.ReadAlways;

			Load();
		}

		public bool ExportToJson()
		{
			try
			{
				fastJSON.JSON.Parameters.UseExtensions = false;
				var json =
				//Newtonsoft.Json.JsonConvert.SerializeObject(Structure, Newtonsoft.Json.Formatting.Indented);
				fastJSON.JSON.ToNiceJSON(Schema);
				//
				io.File.WriteAllText(SchemaJsonFilePath, json);
				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		/// <summary>
		/// Re load the __tables.json structure of the Csv database
		/// </summary>
		/// <param name="dif">for testings only, will be removed</param>
		/// <returns></returns>
		public bool Load()
		{
			try
			{
				//var text = io.File.ReadAllText(FileStructurePath);

				////Newtonsoft.Json.JsonConvert.DeserializeObject<CsvDbStructure>(text);
				////fastJSON is 2X+ faster than Newtonsoft.Json parser
				////https://github.com/mgholam/fastJSON

				//Structure =
				//	fastJSON.JSON.ToObject<DbHeaderStructure>(text);

				var reader = new io.BinaryReader(io.File.OpenRead(SchemaFilePath));
				Schema = DbSchemaConfig.Load(reader);
				reader.Dispose();

				//link
				Tables.ForEach(table =>
				{
					table.Database = this;
					table.Columns.ForEach(column =>
					{
						//update
						if (!column.Indexed)
						{
							column.NodePages = 0;
							column.ItemPages = 0;
						}
						column.Table = table;
					});
				});

				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		public bool Save()
		{
			try
			{
				var writer = new io.BinaryWriter(io.File.Create(SchemaFilePath));
				Schema.Save(writer);
				writer.Dispose();

				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		public DbTable Table(string tableName)
		{
			return Tables.FirstOrDefault(t => String.Compare(t.Name, tableName, true) == 0);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		public DbColumn Index(string tableName, string columnName)
		{
			return Table(tableName)?.Columns.FirstOrDefault(c => String.Compare(c.Name, columnName) == 0);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableColumnName">table.index</param>
		/// <returns></returns>
		public DbColumn Index(string tableColumnName)
		{
			var cols = tableColumnName?.Split('.', StringSplitOptions.RemoveEmptyEntries);
			DbColumn index = null;
			if (cols != null && cols.Length == 2)
			{
				index = Index(cols[0], cols[1]);
			}
			return index;
		}

		public override string ToString()
		{
			return $"{Name} ({Tables?.Count}) table(s)";
		}

		public void Dispose()
		{
			//save main info
			Save();
			//update page frequency info and save
			//var path = io.Path.Combine(BinaryPath, "");

		}
	}

	[Serializable]
	public class DbTable
	{
		public string Name { get; set; }

		public string FileName { get; set; }

		public bool Generate { get; set; }

		public bool Multikey { get; set; }

		public int Rows { get; set; }

		public DbTablePager Pager { get; set; }

		public int Count { get; set; }

		public List<DbColumn> Columns { get; set; }

		protected internal CsvDb Database { get; set; }

		[Newtonsoft.Json.JsonIgnore]
		public Type Type
		{
			get
			{
				var key = Columns.FirstOrDefault(col => col.Key);
				return (key == null) ? null : Type.GetType($"System.{key.Type}");
			}
		}

		public DbColumn Column(string name) => Columns.FirstOrDefault(c => c.Name == name);

		public override string ToString() => $"{Name} ({Count})";

		public IDictionary<string, Type> ToDictionary()
		{
			return new Dictionary<string, Type>(
				Columns.Select(c => new KeyValuePair<string, Type>(c.Name, Type.GetType($"System.{c.Type}")))
			);
		}

	}

	[Serializable]
	public class DbColumn
	{
		public string Indexer { get; set; }

		public bool Unique { get; set; }

		public string Name { get; set; }

		public int Index { get; set; }

		public string Type { get; set; }

		public bool Key { get; set; }

		public bool Indexed { get; set; }

		public int PageCount { get; set; }

		public int ItemPages { get; set; }

		public int NodePages { get; set; }

		[Newtonsoft.Json.JsonIgnore]
		public DbColumnTypeEnum TypeEnum
		{
			get
			{
				if (Enum.TryParse<DbColumnTypeEnum>(Type, out DbColumnTypeEnum type))
				{
					return type;
				}
				return DbColumnTypeEnum.None;
			}
		}

		[System.Xml.Serialization.XmlIgnore]
		//[ScriptIgnore]
		public DbTable Table { get; internal set; }

		private object _itemsIndex;

		public DbIndexItems<T> IndexItems<T>()
			where T : IComparable<T>
		{
			//made [protected internal] to remove type checking later
			//check type
			var typeName = typeof(T).Name; // default(T).GetType().Name;
			if (typeName != Type)
			{
				throw new ArgumentException($"Invalid index type [{typeName}] for column [{Name}]");
			}
			if (_itemsIndex == null)
			{
				_itemsIndex = Activator.CreateInstance(typeof(DbIndexItems<T>), new object[]
				 {
					 Table.Database,
					 Table.Name,
					 Name
				 });
			}
			return (DbIndexItems<T>)_itemsIndex;
		}

		private object _indexTree;

		public DbIndexTree<T> IndexTree<T>()
			where T : IComparable<T>
		{
			//made [protected internal] to remove type checking later
			//check type
			var typeName = typeof(T).Name;
			if (typeName != Type)
			{
				throw new ArgumentException($"Invalid index type [{typeName}] for column [{Name}]");
			}
			if (_indexTree == null)
			{
				_indexTree = Activator.CreateInstance(typeof(DbIndexTree<T>), new object[]
				 {
					 Table.Database,
					 Table.Name,
					 Name
				 });
			}
			return (DbIndexTree<T>)_indexTree;
		}

		public DbColumn() { }

		public override string ToString() => $"[{Index}] {Name}: {Type}{(Key ? " [Key]" : "")}";
	}

	[Serializable]
	public class DbTablePager
	{
		public int PagerSize { get; set; }

		public int Count { get; set; }

		public string File { get; set; }

		public DbTablePager() { }

		public static DbTablePager Load(io.BinaryReader reader)
		{
			return new DbTablePager()
			{
				PagerSize = reader.ReadInt32(),
				Count = reader.ReadInt32(),
				File = reader.BinaryRead()
			};
		}

		public void Store(io.BinaryWriter writer)
		{
			writer.Write(PagerSize);
			writer.Write(Count);
			File.BinarySave(writer);
		}

		public override string ToString()
		{
			return $"{File} PageSize: {PagerSize} Count: {Count}";
		}
	}

}
