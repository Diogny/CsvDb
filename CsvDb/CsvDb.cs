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

	public class CsvDb : IDisposable
	{
		/// <summary>
		/// logging path
		/// </summary>
		public string LogPath { get; protected internal set; }

		/// <summary>
		/// binary files path
		/// </summary>
		public string BinaryPath { get; protected internal set; }

		/// <summary>
		/// database header flags
		/// </summary>
		public DbSchemaConfigType Flags { get { return Schema == null ? DbSchemaConfigType.None : Schema.Flags; } }

		/// <summary>
		/// true if it's a binary schema
		/// </summary>
		public bool IsBinary { get { return (Flags & DbSchemaConfigType.Binary) != 0; } }

		/// <summary>
		/// true if it's a text/csv schema
		/// </summary>
		public bool IsCsv { get { return (Flags & DbSchemaConfigType.Csv) != 0; } }

		/// <summary>
		/// database name
		/// </summary>
		public string Name { get { return Schema?.Name; } }

		/// <summary>
		/// database schema page size
		/// </summary>
		public int PageSize { get { return Schema == null ? -1 : Schema.PageSize; } }

		/// <summary>
		/// read policy
		/// </summary>
		public DbIndexItemsPolicy ReadPolicy { get; set; }

		/// <summary>
		/// schema props
		/// </summary>
		internal DbSchemaConfig Schema;

		/// <summary>
		/// Reference to tables
		/// </summary>
		public IEnumerable<DbTable> Tables
		{
			get { return Schema?._tables.Select(t => t.Value); }
		}

		/// <summary>
		/// true if schema or data modified
		/// </summary>
		public bool Modified { get; set; }

		public static String SchemaSystemName => "__schema";

		public static String SchemaSystemExtension => "bin";

		public static String SchemaJsonName => "__tables";

		public static String SchemaJsonExtension => "json";

		/// <summary>
		/// binary schema filename
		/// </summary>
		public static String SchemaSystemFilename => $"{SchemaSystemName}.{SchemaSystemExtension}";

		/// <summary>
		/// json schema filename
		/// </summary>
		public static String SchemaJsonFilename => $"{SchemaJsonName}.{SchemaJsonExtension}";

		/// <summary>
		/// [table name].data
		/// </summary>
		public static String SchemaTableDataExtension => "data";

		/// <summary>
		/// [table name].csv
		/// </summary>
		public static String SchemaTableDefaultExtension => "csv";

		/// <summary>
		/// binary schema file path of opened database
		/// </summary>
		protected internal String SchemaFilePath = String.Empty;

		/// <summary>
		/// json schema file path of opened database
		/// </summary>
		protected internal String SchemaJsonFilePath = String.Empty;

		/// <summary>
		/// Creates a Csv database schema from a json file
		/// </summary>
		/// <param name="jsonfilepath">json file path</param>
		/// <param name="flags">creation schema flags</param>
		/// <returns></returns>
		public static CsvDb CreateFromJson(string jsonfilepath, DbSchemaConfigType flags = DbSchemaConfigType.None)
		{
			//generate the __schema.bin file and create normal database
			var text = io.File.ReadAllText(jsonfilepath);

			////Newtonsoft.Json.JsonConvert.DeserializeObject<CsvDbStructure>(text);
			////fastJSON is 4X+ faster than Newtonsoft.Json parser
			////https://github.com/mgholam/fastJSON

			var schema = fastJSON.JSON.ToObject<DbSchemaConfig>(text);

			//define paths
			var rootPath = io.Path.GetDirectoryName(jsonfilepath);
			var sysPath = io.Path.Combine(rootPath, $"{SchemaSystemFilename}");

			//set flags
			schema.Flags = flags;

			//save
			var writer = new io.BinaryWriter(io.File.Create(sysPath));
			schema.Save(writer);
			writer.Dispose();

			//remove the ending \bin\
			return new CsvDb(io.Path.GetDirectoryName(rootPath));
		}

		/// <summary>
		/// Creates a new CSV database
		/// </summary>
		/// <param name="path">path to the root of the databse</param>
		/// <param name="dif">for testings only, will be removed</param>
		public CsvDb(string path)
		{

			// path\bin\
			//		__schema.bin		// => old  __tables.json
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

		/// <summary>
		/// Export the database schema as json to disk
		/// </summary>
		/// <param name="jsonpath">json file path</param>
		/// <returns></returns>
		public bool ExportToJson(string jsonpath)
		{
			try
			{
				fastJSON.JSON.Parameters.UseExtensions = false;
				var json =
				//Newtonsoft.Json.JsonConvert.SerializeObject(Structure, Newtonsoft.Json.Formatting.Indented);
				fastJSON.JSON.ToNiceJSON(Schema);
				//
				io.File.WriteAllText(jsonpath, json);
				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		/// <summary>
		/// Re load the schema structure of the database from disk
		/// </summary>
		/// <returns>true if loaded successfully</returns>
		public bool Load()
		{
			try
			{
				var reader = new io.BinaryReader(io.File.OpenRead(SchemaFilePath));
				Schema = DbSchemaConfig.Load(reader);
				reader.Dispose();

				//link
				foreach (var table in Tables)
				{
					table.Database = this;
					foreach (var column in table.Columns)
					{
						//update
						if (!column.Indexed)
						{
							column.NodePages = 0;
							column.ItemPages = 0;
						}
						column.Table = table;
					}
				};

				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		/// <summary>
		/// Saves the database to disk
		/// </summary>
		/// <returns></returns>
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

		/// <summary>
		/// Gets the table
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <returns></returns>
		public DbTable Table(string tableName) => this[tableName];

		/// <summary>
		/// Get the table
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <returns></returns>
		public DbTable this[string tableName] => Schema[tableName];

		/// <summary>
		/// Gets the table column
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		public DbColumn Index(string tableName, string columnName)
		{
			return Table(tableName)?.Columns.FirstOrDefault(c => String.Compare(c.Name, columnName) == 0);
		}

		/// <summary>
		/// Gets the table column
		/// </summary>
		/// <param name="hash">table.column</param>
		/// <returns></returns>
		public DbColumn Index(string hash)
		{
			var cols = hash?.Split('.', StringSplitOptions.RemoveEmptyEntries);
			DbColumn index = null;
			if (cols != null && cols.Length == 2)
			{
				index = Index(cols[0], cols[1]);
			}
			return index;
		}

		public override string ToString() => $"{Name} ({Schema.Count}) table(s)";

		public void Dispose()
		{
			//save main info
			Save();
			//update page frequency info and save
			//var path = io.Path.Combine(BinaryPath, "");

		}
	}

	/// <summary>
	/// schema configuration class
	/// </summary>
	internal class DbSchemaConfig
	{
		/// <summary>
		/// Schema header flags
		/// </summary>
		public DbSchemaConfigType Flags { get; set; }

		/// <summary>
		/// Schema version
		/// </summary>
		public String Version { get; set; }

		//Major.Minor.Build.Revision	3.5.234.02
		//[Newtonsoft.Json.JsonProperty(Required = Newtonsoft.Json.Required.Default)]
		[Newtonsoft.Json.JsonIgnore]
		[System.Xml.Serialization.XmlIgnore]
		public System.Version VersionInfo => new System.Version(Version);

		/// <summary>
		/// Schema description
		/// </summary>
		public String Description { get; set; }

		/// <summary>
		/// Schema name
		/// </summary>
		public String Name { get; set; }

		/// <summary>
		/// Schema page size
		/// </summary>
		public int PageSize { get; set; }

		internal Dictionary<string, DbTable> _tables;

		/// <summary>
		/// Amount of tables in the schema
		/// </summary>
		public int Count => _tables == null ? 0 : _tables.Count;

		/// <summary>
		/// gets the table if exists, otherwise null
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <returns></returns>
		public DbTable this[string tableName] => _tables.TryGetValue(tableName, out DbTable table) ? table : null;

		public DbSchemaConfig() { }

		/// <summary>
		/// Loads a database schema
		/// </summary>
		/// <param name="reader">binary reader</param>
		/// <returns></returns>
		public static DbSchemaConfig Load(io.BinaryReader reader)
		{
			var schema = new DbSchemaConfig()
			{
				Flags = (DbSchemaConfigType)reader.ReadUInt32(),
				Version = reader.BinaryRead(),
				Description = reader.BinaryRead(),
				Name = reader.BinaryRead(),

				PageSize = reader.ReadInt32(),
				_tables = new Dictionary<string, DbTable>()
				//Tables = new List<DbTable>()
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
					RowMask = reader.ReadUInt64(),
					RowMaskLength = reader.ReadInt32(),
					Pager = DbTablePager.Load(reader),
					Count = reader.ReadInt32(),
					_columns = new Dictionary<string, DbColumn>()
				};
				//read columns
				var columnKeyList = new List<string>();

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
						Indexed = reader.ReadBoolean(),
						//
						NodePages = reader.ReadInt32(),
						ItemPages = reader.ReadInt32()
					};
					if (!table.Add(col))
					{
						throw new ArgumentException($"duplicated column: {col.Name} on table {table.Name}");
					}
					columnKeyList.Add(col.Name);
				}
				//check count
				if (table.Count != table.Columns.Count())
				{
					throw new ArgumentException($"invalid table count on: {table.Name}");
				}
				table._columnKeyList = columnKeyList.ToArray();

				//schema.Tables.Add(table);
				schema._tables.Add(table.Name, table);
			}
			return schema;
		}

		//struct MetaPageFrequency
		//{
		//	public int Offset;

		//	public double Frequency;
		//}

		/// <summary>
		/// Saves the database schema
		/// </summary>
		/// <param name="writer">binary writer</param>
		public void Save(io.BinaryWriter writer)
		{
			//flags
			writer.Write((UInt32)Flags);
			//
			Version.BinarySave(writer);
			Description.BinarySave(writer);
			Name.BinarySave(writer);

			//page size
			writer.Write(PageSize);

			//tables
			int pageCount = _tables.Count; // Tables.Count;
			writer.Write(pageCount);

			//frequency
			//var frequencyList = new List<KeyValuePair<string, List<MetaPageFrequency>>>();

			foreach (var table in _tables.Select(t => t.Value)) // Tables
			{
				table.Name.BinarySave(writer);
				table.FileName.BinarySave(writer);
				writer.Write(table.Generate);
				writer.Write(table.Multikey);
				//
				writer.Write(table.Rows);

				writer.Write(table.RowMask);
				writer.Write(table.RowMaskLength);

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
					//
					writer.Write(column.NodePages);
					writer.Write(column.ItemPages);

					//frequency
					//var hash = $"{table.Name}.{column.Name}";
					//var freq = new KeyValuePair<string, List<MetaPageFrequency>>(hash, new List<MetaPageFrequency>());
					//frequencyList.Add(freq);

					
				}

			}

			//save frequency data to disk

		}

	}

	/// <summary>
	/// Represents a database table
	/// </summary>
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

		internal string[] _columnKeyList;

		internal Dictionary<string, DbColumn> _columns;
		/// <summary>
		/// get all columns from table
		/// </summary>
		public IEnumerable<DbColumn> Columns => _columns.Values;

		/// <summary>
		/// indexer for columns
		/// </summary>
		/// <param name="name">column name</param>
		/// <returns></returns>
		public DbColumn this[string name] =>
			_columns.TryGetValue(name, out DbColumn column) ? column : null;

		/// <summary>
		/// indexer for columns
		/// </summary>
		/// <param name="position">0-based column position</param>
		/// <returns></returns>
		public DbColumn this[int position] =>
			(position >= 0 && position < _columnKeyList.Length) ? this[_columnKeyList[position]] : null;

		/// <summary>
		/// adds a new column to the table
		/// </summary>
		/// <param name="column">column</param>
		/// <returns></returns>
		internal bool Add(DbColumn column)
		{
			if (column == null || this[column.Name] != null)
			{
				return false;
			}
			_columns.Add(column.Name, column);
			return true;
		}

		//[Newtonsoft.Json.JsonProperty(Required = Newtonsoft.Json.Required.Default)]
		[Newtonsoft.Json.JsonIgnore]
		[System.Xml.Serialization.XmlIgnore]
		protected internal CsvDb Database { get; set; }

		/// <summary>
		/// Mask of nullable rows
		/// </summary>
		public UInt64 RowMask { get; set; }

		/// <summary>
		/// bytes of RowMask
		/// </summary>
		public int RowMaskLength { get; set; }

		[Newtonsoft.Json.JsonIgnore]
		[System.Xml.Serialization.XmlIgnore]
		public DbColumnType[] ColumnTypes => Columns.Select(c => Enum.Parse<DbColumnType>(c.Type)).ToArray();

		[Newtonsoft.Json.JsonIgnore]
		[System.Xml.Serialization.XmlIgnore]
		public Type Type
		{
			get
			{
				var key = Columns.FirstOrDefault(col => col.Key);
				return (key == null) ? null : Type.GetType($"System.{key.Type}");
			}
		}

		//public DbColumn Column(string name) => Columns.FirstOrDefault(c => c.Name == name);

		public override string ToString() => $"{Name} ({Count})";

		public IDictionary<string, Type> ToDictionary()
		{
			return new Dictionary<string, Type>(
				Columns.Select(c => new KeyValuePair<string, Type>(c.Name, Type.GetType($"System.{c.Type}")))
			);
		}

	}

	/// <summary>
	/// Represents a database table column
	/// </summary>
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

		public int ItemPages { get; set; }

		public int NodePages { get; set; }

		/// <summary>
		/// Returns the sum of item pages plus tree node pages
		/// </summary>
		[System.Xml.Serialization.XmlIgnore]
		[Newtonsoft.Json.JsonIgnore]
		public int PageCount { get { return ItemPages + NodePages; } }

		[System.Xml.Serialization.XmlIgnore]
		[Newtonsoft.Json.JsonIgnore]
		public DbColumnType TypeEnum
		{
			get
			{
				return (Enum.TryParse(Type, out DbColumnType type)) ? type : DbColumnType.None;
			}
		}

		[System.Xml.Serialization.XmlIgnore]
		[Newtonsoft.Json.JsonIgnore]
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

		public override string ToString() => $"[{Index}] {Name}: {Type}{(Key ? " [Key]" : "")}";
	}

	/// <summary>
	/// Represents a database table pager
	/// </summary>
	[Serializable]
	public class DbTablePager
	{
		public int PagerSize { get; set; }

		public int Count { get; set; }

		public string File { get; set; }

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
