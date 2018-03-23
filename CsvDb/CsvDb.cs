using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	[Flags]
	public enum CsvDbOption
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

	public enum CsvDbIndexItemsPolicy
	{
		ReadAlways,
		ReadAndStoreAlways,
		ReadKeepMostFrequentAlways
	}

	internal class CsvDbStructure
	{
		public String Version { get; set; }

		public String Description { get; set; }

		public String Name { get; set; }

		public int PageSize { get; set; }

		public List<CsvDbTable> Tables { get; set; }

	}

	public class CsvDb : IDisposable
	{
		public string LogPath { get; protected internal set; }

		public string BinaryPath { get; protected internal set; }

		protected internal String FileStructurePath = "";

		public string Name { get { return Structure.Name; } }

		public int PageSize { get { return Structure.PageSize; } }

		public CsvDbIndexItemsPolicy ReadPolicy { get; set; }

		internal CsvDbStructure Structure;

		/// <summary>
		/// Reference to tables
		/// </summary>
		public List<CsvDbTable> Tables
		{
			get { return Structure.Tables; }
			set { Structure.Tables = value; }
		}

		public bool Modified { get; set; }

		public static String SystemStructureFile { get { return "__tables.json"; } }

		public CsvDb(string structure, string path)
		{
			if (String.IsNullOrWhiteSpace(structure))
			{
				throw new ArgumentException($"Invalid databse structure");
			}
			//Newtonsoft.Json.JsonConvert.DeserializeObject<CsvDbStructure>(text);
			//fastJSON is 2X+ faster than Newtonsoft.Json parser
			//https://github.com/mgholam/fastJSON

			Structure =
				fastJSON.JSON.ToObject<CsvDbStructure>(structure);

			ReadPolicy = CsvDbIndexItemsPolicy.ReadAlways;

			//link
			Tables.ForEach(t =>
			{
				t.Database = this;
				t.Columns.ForEach(c =>
				{
					c.Table = t;
				});
			});
			//test for [LogPath] root path
			if (!io.Directory.Exists(LogPath = path))
			{
				io.Directory.CreateDirectory(LogPath);
			}
			//test for: [LogPath]\bin    system tables bin path
			if (!io.Directory.Exists(BinaryPath = io.Path.Combine(LogPath, "bin\\")))
			{
				io.Directory.CreateDirectory(BinaryPath);
			}
			FileStructurePath = io.Path.Combine(BinaryPath, SystemStructureFile);
			Save();
		}

		/// <summary>
		/// Creates a new CSV database
		/// </summary>
		/// <param name="path">path to the root of the databse</param>
		/// <param name="dif">for testings only, will be removed</param>
		public CsvDb(string path)
		{

			// path\bin\
			//		__tables.json
			//		[table].csv
			//		[table].pager
			//		[table].[index].index
			//		[table].[index].index.bin
			//
			// path\
			//		[table].[index].index.txt
			//		[table].[index].index.tree.txt
			//		[table].[index].index.duplicates.txt
			//
			//		[table].log

			//point path to path\bin\
			BinaryPath = io.Path.Combine(LogPath = path, "bin\\");

			//read 
			FileStructurePath = io.Path.Combine(BinaryPath, SystemStructureFile);
			if (!io.File.Exists(FileStructurePath))
			{
				throw new ArgumentException($"cannot load database structure on: {FileStructurePath}");
				////create an empty __tables.json file
				//if (!io.Directory.Exists(BinaryPath))
				//{
				//	io.Directory.CreateDirectory(BinaryPath);

				//	//later add option to clean directory with flag if dessired

				//}

				//var obj = new
				//{
				//	Version = "1.01",
				//	Description = "Implementing a system hidden variables",
				//	PageSize = 255,
				//	Tables = new String[0]
				//};
				//var json =
				//			 Newtonsoft.Json.JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented);
				////
				//io.File.WriteAllText(FileStructurePath, json);
			}
			//Name = io.Path.GetFileNameWithoutExtension(Path = path);
			//

			ReadPolicy = CsvDbIndexItemsPolicy.ReadAlways;

			Load();
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
				var text = io.File.ReadAllText(FileStructurePath);

				//Newtonsoft.Json.JsonConvert.DeserializeObject<CsvDbStructure>(text);
				//fastJSON is 2X+ faster than Newtonsoft.Json parser
				//https://github.com/mgholam/fastJSON

				Structure =
					fastJSON.JSON.ToObject<CsvDbStructure>(text);

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
				fastJSON.JSON.Parameters.UseExtensions = false;
				var json =
				//Newtonsoft.Json.JsonConvert.SerializeObject(Structure, Newtonsoft.Json.Formatting.Indented);
				fastJSON.JSON.ToNiceJSON(Structure);
				//
				io.File.WriteAllText(FileStructurePath, json);
				return true;
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
				return false;
			}
		}

		public CsvDbTable Table(string tableName)
		{
			return Tables.FirstOrDefault(t => String.Compare(t.Name, tableName, true) == 0);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		public CsvDbColumn Index(string tableName, string columnName)
		{
			return Table(tableName)?.Columns.FirstOrDefault(c => String.Compare(c.Name, columnName) == 0);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableColumnName">table.index</param>
		/// <returns></returns>
		public CsvDbColumn Index(string tableColumnName)
		{
			var cols = tableColumnName?.Split('.', StringSplitOptions.RemoveEmptyEntries);
			CsvDbColumn index = null;
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
	public class CsvDbTable
	{
		public string Name { get; set; }

		public string FileName { get; set; }

		public bool Generate { get; set; }

		public bool Multikey { get; set; }

		public int Rows { get; set; }

		public CsvDbTablePager Pager { get; set; }

		public int Count { get; set; }

		public List<CsvDbColumn> Columns { get; set; }

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

		public CsvDbColumn Column(string name)
		{
			return Columns.FirstOrDefault(c => c.Name == name);
		}

		public override string ToString()
		{
			return $"{Name} ({Count})";
		}
	}

	[Serializable]
	public class CsvDbColumn
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
		public CsvDbColumnTypeEnum TypeEnum
		{
			get
			{
				if (Enum.TryParse<CsvDbColumnTypeEnum>(Type, out CsvDbColumnTypeEnum type))
				{
					return type;
				}
				return CsvDbColumnTypeEnum.None;
			}
		}

		[System.Xml.Serialization.XmlIgnore]
		//[ScriptIgnore]
		public CsvDbTable Table { get; internal set; }

		private object _itemsIndex;

		public CsvDbIndexItems<T> IndexItems<T>()
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
				_itemsIndex = Activator.CreateInstance(typeof(CsvDbIndexItems<T>), new object[]
				 {
					 Table.Database,
					 Table.Name,
					 Name
				 });
			}
			return (CsvDbIndexItems<T>)_itemsIndex;
		}

		private object _indexTree;

		public CsvDbIndexTree<T> IndexTree<T>()
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
				_indexTree = Activator.CreateInstance(typeof(CsvDbIndexTree<T>), new object[]
				 {
					 Table.Database,
					 Table.Name,
					 Name
				 });
			}
			return (CsvDbIndexTree<T>)_indexTree;
		}

		public CsvDbColumn()
		{ }

		public override string ToString()
		{
			return $"[{Index}] {Name}: {Type}{(Key ? " [Key]" : "")}";
		}
	}

	[Serializable]
	public class CsvDbTablePager
	{
		public int PagerSize { get; set; }

		public int Count { get; set; }

		public string File { get; set; }

		public override string ToString()
		{
			return $"{File} PageSize: {PagerSize} Count: {Count}";
		}
	}

}
