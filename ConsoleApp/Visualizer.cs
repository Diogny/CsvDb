using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using io = System.IO;
using db = CsvDb;
using consts = CsvDb.Consts;

namespace ConsoleApp
{
	/// <summary>
	/// A Simple test class, due for deleting soon
	/// </summary>
	public class Visualizer
	{

		public db.CsvDb Database { get; set; }

		public Visualizer(db.CsvDb db)
		{
			Database = db;
		}

		public void TestIndexItems()
		{
			Console.WriteLine("Tree Item Pages\n");

			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\nProcessing [{table.Name}] index(es)");

				foreach (var col in table.Columns.Where(c => c.Indexed))
				{
					var pathTree = io.Path.Combine(Database.BinaryPath, $"{col.Indexer}");
					var pathPages = $"{pathTree}.bin";

					if (!io.File.Exists(pathPages))
					{
						Console.WriteLine("\t Tree Item [{col.Name}] index not found, skip!");
					}
					else
					{
						using (var reader = new io.BinaryReader(io.File.OpenRead(pathPages)))
						{
							Int32 pageCount = reader.ReadInt32();
							Console.WriteLine($"\n\tIndex [{col.Name}]\t Page Count: {pageCount}");

							Int32 keyTypeValue = reader.ReadInt32();
							var keyType = (db.CsvDbColumnTypeEnum)keyTypeValue;

							var ii = 0;

							for (var pi = 0; pi < pageCount; pi++)
							{
								var flags = reader.ReadInt32();
								var pageType = flags & 0b011;

								if (pageType != consts.BTreePageItemsFlag)
								{
									Console.WriteLine($"\t invalid page type...");
									break;
								}
								var uniqueKeyValue = (flags & consts.BTreeUniqueKeyValueFlag) != 0;

								var offset = reader.ReadInt32();

								var pageSize = reader.ReadInt32();

								var itemsCount = reader.ReadInt32();

								Console.WriteLine($"\t\t[{pi + 1}]\tFlags: {flags}, Offset: {offset}, Page Size: {pageSize}, Item Count: {itemsCount}, Unique Key Values: {uniqueKeyValue}");

								//keys
								var keyvaluepairs = new List<KeyValuePair<object, List<int>>>();
								KeyValuePair<object, List<int>> pair;

								if (keyType == db.CsvDbColumnTypeEnum.String)
								{
									var keyLengths = new byte[itemsCount];
									reader.Read(keyLengths, 0, itemsCount);
									//
									for (ii = 0; ii < itemsCount; ii++)
									{
										var charArray = new char[keyLengths[ii]];
										reader.Read(charArray, 0, keyLengths[ii]);
										//
										pair = new KeyValuePair<object, List<int>>(
											new String(charArray),
											new List<int>());
										keyvaluepairs.Add(pair);
									}
								}
								else
								{
									for (ii = 0; ii < itemsCount; ii++)
									{
										var key = db.Utils.LoadKey(keyType, reader);
										pair = new KeyValuePair<object, List<int>>(
											key,
											new List<int>());
										keyvaluepairs.Add(pair);
									}
								}

								//values
								if (uniqueKeyValue)
								{
									for (ii = 0; ii < itemsCount; ii++)
									{
										keyvaluepairs[ii].Value.Add(reader.ReadInt32());
									}
								}
								else
								{
									for (ii = 0; ii < itemsCount; ii++)
									{
										Int16 itemLen = reader.ReadInt16();
										var itemList = keyvaluepairs[ii].Value;
										for (var ip = 0; ip < itemLen; ip++)
										{
											itemList.Add(reader.ReadInt32());
										}
									}
								}
								Console.Write("\t\t Keys:");
								var keyjoin = String.Join(", ", keyvaluepairs.Select(p => p.Key));
								//
								var trimmed = Math.Min(Math.Max(128, keyjoin.Length), keyjoin.Length);

								Console.WriteLine($"{keyjoin.Substring(0, trimmed)}{((trimmed < keyjoin.Length) ? "..." : "")}");
								//too big
								foreach (var p in keyvaluepairs)
								{
									//Console.WriteLine($"\t  {p.Key}, {String.Join(", ", p.Value)}");
								}

							}
						}

					}
				}
			}
		}

		public void TestIndexTreeShape()
		{
			Console.WriteLine("Tree Node Pages\n");

			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\nProcessing [{table.Name}] index(es)");

				foreach (var col in table.Columns.Where(c => c.Indexed))
				{
					Console.WriteLine($"\nIndex [{col.Name}]");
					var pathTree = io.Path.Combine(Database.BinaryPath, $"{col.Indexer}");
					var pathPages = $"{pathTree}.bin";

					if (!io.File.Exists(pathTree))
					{
						Console.WriteLine("Tree index not found, skip!");
					}
					else
					{
						Console.WriteLine("Header:");
						using (var reader = new io.BinaryReader(io.File.OpenRead(pathTree)))
						{
							//header CsvDbColumnHeader
							//var header = CsvDbColumnHeader.FromArray()
							var headerBuffer = new byte[db.CsvDbColumnHeader.Size];
							//
							var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
							var header = db.CsvDbColumnHeader.FromArray(headerBuffer);

							Console.WriteLine($"Value0: {header.Value0}");
							Console.WriteLine($"Column Index: {header.ColumnIndex}");
							Console.WriteLine($"Page Count: {header.PageCount}");
							Console.WriteLine($"Flags: {header.Flags}");

							var isUnique = (header.Flags & consts.IndexHeaderIsUnique) != 0;
							var isKey = (header.Flags & consts.IndexHeaderIsKey) != 0;
							var isLeaf = (header.Flags & consts.IndexHeaderIsLeaf) != 0;

							byte keyTypeValue = (byte)header.Flags;
							var keyType = (db.CsvDbColumnTypeEnum)keyTypeValue;

							Console.WriteLine($" Unique: {isUnique}");
							Console.WriteLine($" Key: {isKey}");
							Console.WriteLine($" Leaf: {isLeaf}");
							Console.WriteLine($" Key Type: {keyType}\n");

							if (!isLeaf)
							{
								ReadTreePageShape("", "", reader, keyType);
							}
						}
					}
				}
			}
		}

		void ReadTreePageShape(
			string keyprefix,
			string childrenprefix,
			io.BinaryReader reader,
			db.CsvDbColumnTypeEnum keyType)
		{
			var flags = reader.ReadInt32();

			//remove last 3 chars if any
			var newCldPref = childrenprefix.Length == 0 ?
						 "" :
						 childrenprefix.Substring(0, childrenprefix.Length - 3);

			var prefix = $"{newCldPref}{keyprefix}";


			var pageType = flags & 0b011;
			switch (pageType)
			{
				case consts.BTreePageNodeFlag:
					var pageSize = reader.ReadInt32();

					var uniqueKeyValue = (flags & consts.BTreeUniqueKeyValueFlag) != 0;

					Int32 keyValue = 0;
					Int32 keyValueCount = 0;
					var keyValueCollection = new List<int>();

					//read value(s)
					if (uniqueKeyValue)
					{
						keyValue = reader.ReadInt32();
					}
					else
					{
						keyValueCount = reader.ReadInt32();
						for (var i = 0; i < keyValueCount; i++)
						{
							keyValueCollection.Add(reader.ReadInt32());
						}
					}

					//read key
					var key = db.Utils.LoadKey(keyType, reader);

					Console.WriteLine($"{prefix}Key: <{key}> PageSize: {pageSize}, Unique Key Value: {uniqueKeyValue}");

					//left tree page node
					ReadTreePageShape("├──", childrenprefix + "│  ", reader, keyType);

					//right tree page node
					ReadTreePageShape("└──", childrenprefix + "   ", reader, keyType);

					break;
				case consts.BTreePageNodeItemsFlag:
					//read offset to [table][column].index.bin for page items
					var offset = reader.ReadInt32();
					Console.WriteLine($"{prefix}PageItem, Offset to item page: {offset}");
					break;
				default:
					throw new ArgumentException("Invalid database structure!");
			}
		}

		public void TestIndexTrees()
		{
			Console.WriteLine("Tree Node Pages\n");

			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\nProcessing [{table.Name}] index(es)");

				foreach (var col in table.Columns.Where(c => c.Indexed))
				{
					Console.WriteLine($"\n\tIndex [{col.Name}]");
					var pathTree = io.Path.Combine(Database.BinaryPath, $"{col.Indexer}");
					var pathPages = $"{pathTree}.bin";

					if (!io.File.Exists(pathTree))
					{
						Console.WriteLine("\t Tree index not found, skip!");
					}
					else
					{
						Console.WriteLine("\t Tree:");
						using (var reader = new io.BinaryReader(io.File.OpenRead(pathTree)))
						{
							//header CsvDbColumnHeader
							//var header = CsvDbColumnHeader.FromArray()
							var headerBuffer = new byte[db.CsvDbColumnHeader.Size];
							//
							var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
							var header = db.CsvDbColumnHeader.FromArray(headerBuffer);

							Console.WriteLine($"\tValue0: {header.Value0}");
							Console.WriteLine($"\tColumn Index: {header.ColumnIndex}");
							Console.WriteLine($"\tPage Count: {header.PageCount}");
							Console.WriteLine($"\tFlags: {header.Flags}");

							var isUnique = (header.Flags & consts.IndexHeaderIsUnique) != 0;
							var isKey = (header.Flags & consts.IndexHeaderIsKey) != 0;
							var isLeaf = (header.Flags & consts.IndexHeaderIsLeaf) != 0;

							byte keyTypeValue = (byte)header.Flags;
							var keyType = (db.CsvDbColumnTypeEnum)keyTypeValue;

							Console.WriteLine($"\t Unique: {isUnique}");
							Console.WriteLine($"\t Key: {isKey}");
							Console.WriteLine($"\t Leaf: {isLeaf}");
							Console.WriteLine($"\t Key Type: {keyType}");
							Console.WriteLine($"\t");

							if (!isLeaf)
							{
								Console.WriteLine("\t Tree Page(s):");
								pageId = 0;
								readTreePage(0, reader, keyType);
							}
						}
					}
				}
			}
		}

		int pageId = 0;

		void readTreePage(int parent, io.BinaryReader reader, db.CsvDbColumnTypeEnum keyType)
		{
			var flags = reader.ReadInt32();
			//
			var thisPageNo = ++pageId;

			var pageType = flags & 0b011;
			switch (pageType)
			{
				case consts.BTreePageNodeFlag:
					Console.WriteLine($"\t>PageNode [{thisPageNo}]");
					var pageSize = reader.ReadInt32();

					var uniqueKeyValue = (flags & consts.BTreeUniqueKeyValueFlag) != 0;

					Console.WriteLine($"\t Parent: {parent}, PageSize: {pageSize}, Unique Key Value: {uniqueKeyValue}");

					Int32 keyValue = 0;
					Int32 keyValueCount = 0;
					var keyValueCollection = new List<int>();

					//read value(s)
					if (uniqueKeyValue)
					{
						keyValue = reader.ReadInt32();
					}
					else
					{
						keyValueCount = reader.ReadInt32();
						for (var i = 0; i < keyValueCount; i++)
						{
							keyValueCollection.Add(reader.ReadInt32());
						}
					}

					//read key
					var key = db.Utils.LoadKey(keyType, reader);

					//show key,value(s)
					if (uniqueKeyValue)
					{
						Console.WriteLine($"\t Key: <{key}, {keyValue}>");
					}
					else
					{
						Console.WriteLine($"\t Key: <{key}, ({keyValueCount})[{String.Join(", ", keyValueCollection)}]>");
					}

					//left tree page node
					readTreePage(thisPageNo, reader, keyType);

					//right tree page node
					readTreePage(thisPageNo, reader, keyType);

					break;
				case consts.BTreePageNodeItemsFlag:
					Console.WriteLine($"\t>PageItem [{thisPageNo}]");

					//read offset to [table][column].index.bin for page items
					var offset = reader.ReadInt32();
					//done!
					Console.WriteLine($"\t Parent {parent}, Offset to item page: {offset}");

					break;
				default:
					throw new ArgumentException("Invalid database structure!");
			}
		}

		public void TestOverwrite()
		{
			var stream = new io.MemoryStream();
			var writer = new io.BinaryWriter(stream);

			Int32 value = consts.BTreePageNodeFlag | consts.BTreeUniqueKeyValueFlag;

			writer.Write(value);

			//placeholder
			writer.Write(value = 0);

			//write more dummy things
			byte b = 2;
			writer.Write(b);


			string s = "ABC";
			writer.Write(s);

			writer.Write(b = 16);

			//this is the original stream
			var buffer = stream.ToArray();
			var length = (Int32)stream.Length; // buffer.Length;

			//go back
			stream.Position = 4;
			//overwrite length
			writer.Write(length);

			var buffer2 = stream.ToArray();
		}


	}

}
