using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	public class CsvDbGenerator
	{
		public string ZipFile { get; protected internal set; }

		public CsvDb Database { get; protected internal set; }

		public static Int32 ItemsPageStart => 8;

		public CsvDbGenerator(CsvDb db, string zipfilepath, bool removeAll = true)
		{
			if ((Database = db) == null ||
				!System.IO.File.Exists(ZipFile = zipfilepath) || !zipfilepath.EndsWith(".zip"))
			{
				throw new ArgumentException($"Csv Database db or zip file path donot exists: {zipfilepath}");
			}
			// remove all but the __tables.json file


		}

		#region Text Data Generator

		public void GenerateTxtData()
		{
			var zip = io.Compression.ZipFile.OpenRead(ZipFile);
			//generic method
			MethodInfo processTable_Method =
				this.GetType()
					.GetMethod(nameof(CsvDbGenerator.generateCsvTableText),
					BindingFlags.Instance | BindingFlags.NonPublic);

			foreach (var table in Database.Tables)
			{
				var csvfile = io.Path.Combine(Database.BinaryPath, $"{table.FileName}");

				Console.WriteLine($"\r\n  table: {table.Name}\r\n  processing file ({csvfile})");

				//get zip entry
				var zipentryfilename = io.Path.ChangeExtension(table.FileName, ".txt");

				var entry = zip.GetEntry(zipentryfilename);
				if (entry == null)
				{
					Console.WriteLine($"  cannot find: {table.FileName} CSV file");
					continue;
				}
				Console.WriteLine($"  reading {entry.Length} byte(s)");

				try
				{
					//read CSV file data
					var csv = OpenCsvFile(entry);
					//call generic table processing method
					MethodInfo generic = processTable_Method.MakeGenericMethod(table.Type);
					//invoke
					generic.Invoke(this, new object[] { csv, entry, table, Database.LogPath });
					Console.WriteLine($"  done!");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"error-> {ex.Message}");
				}
			}
		}

		CsvHelper.CsvReader OpenCsvFile(io.Compression.ZipArchiveEntry entry)
		{
			using (var outStream = new io.StreamReader(entry.Open()))
			{
				var content = outStream.ReadToEnd();

				byte[] byteArray = System.Text.Encoding.ASCII.GetBytes(content);

				io.MemoryStream mstream = new io.MemoryStream(byteArray);

				var stream = new io.StreamReader(mstream);

				var csv = new CsvHelper.CsvReader(stream);

				return csv;
			}
		}

		void generateCsvTableText<TClass>(
			CsvHelper.CsvReader csv,
			io.Compression.ZipArchiveEntry entry,
			CsvDbTable table,
			string rootPath)
		{
			string pagerName = $"{table.Name}.pager";
			string pagerFilename = io.Path.Combine(rootPath, $"{pagerName}.txt");
			var pageCount = 1;
			var pageSize = table.Pager.PagerSize;
			//set to 0 initially, but now starts with "1"
			var globalRow = 1;
			var position = 0;

			var prevPosition = 0;
			var prevGlobalRow = 0;

			//page size calculator
			Func<int, int, int> calculatePageSize = (prePos, currPos) => currPos - prePos; // + 1;

			using (var csvPagerwriter = new io.StreamWriter(io.File.Create(pagerFilename)))
			{
				IndexStruct[] indexesArray = null;
				//CSV text writer
				io.StreamWriter csvwriter = null;

				if (!table.Generate)
				{
					Console.WriteLine($"");
				}
				else
				{
					csvwriter = new io.StreamWriter(io.File.Create(io.Path.Combine(rootPath, $"bin\\{table.Name}.csv")));
					Console.WriteLine($"  creating temporary text index files.");
					//create temporary files for csv pager, and all indexes
					indexesArray = (from col in table.Columns
													where col.Indexed
													let tmpfile = io.Path.Combine(rootPath, $"{col.Indexer}.txt")
													select new IndexStruct
													{
														column = col,
														file = tmpfile,
														writer = new io.StreamWriter(io.File.Create(tmpfile))
													}).ToArray();
					Console.WriteLine($"   ({indexesArray.Length}) index(es) found.");
					Console.WriteLine($"{String.Join("", indexesArray.Select(i => $"    -{i.file}\r\n"))}");
				}
				if (!csv.Read())
				{
					Console.WriteLine($"  error: table header expected!");
				}
				Console.WriteLine($"  CSV headers read.");

				//check field headers indexes with table metadata
				var ndx = 0;
				foreach (var col in table.Columns)
				{
					var headcol = csv.GetField(ndx);
					if (headcol != col.Name || ndx != col.Index)
					{
						Console.WriteLine($"  error: table header name and/or ordering is invalid!");
						return;
					}
					ndx++;
				}
				Console.WriteLine($"  CSV headers checked.\r\n  Writing CSV database text file, and generating index files...");
				//
				var tableUniqueKeyCol = table.Columns.Where(c => c.Key).First();
				//
				int rowLine = 0;
				var sb = new StringBuilder();
				var startTime = DateTime.Now;
				var csvParser = new CsvRecordParser(csv, table);

				//read each record of csv entry
				while (csv.Read())
				{
					rowLine++;
					//
					if (table.Generate)
					{
						//get csv record columns string
						csvParser.ReadRecord();
						var csvRow = csvParser.Record();

						//write line to temporary buffer
						sb.Append($"{csvRow}\r\n");

						if (sb.Length > 16 * 1024)
						{
							//write line to the .CSV file
							csvwriter.Write(sb.ToString());
							sb.Length = 0;
						}
						//get unique key value (non-casted)
						var tableUniqueKeyValue = csvParser.Values[tableUniqueKeyCol.Index];

						//process pager
						if (pageSize-- <= 0)
						{
							pageCount++;
							//reset page size
							pageSize = table.Pager.PagerSize;

							//save previous one
							//page header
							//[start row]
							//[start position]
							csvPagerwriter.WriteLine(
								$"{prevGlobalRow}|{prevPosition}|{calculatePageSize(prevPosition, position)}");
							//
							prevGlobalRow = globalRow;
							prevPosition = position;
						}

						//process indexes
						foreach (var index in indexesArray)
						{
							var indexColValue = csvParser.Values[index.column.Index];

							//position index: $"{value}|{position}"
							//row index:			$"{value}|{globalRow}"
							//if index is key, store its position
							//  otherwise its line number 1-based   must be the key value for single key table.
							string indexLine = null;
							if (index.column.Key)
							{
								//for Keys store the position inside the .CSV file
								indexLine = $"{indexColValue}|{position}";
							}
							else
							{
								indexLine = $"{indexColValue}|{tableUniqueKeyValue}";
							}
							//write line
							index.writer.WriteLine(indexLine);
						}

						//calculate values for next row
						globalRow++;
						position += csvRow.Length + 2; // +2 for \r\n

					}

				};
				//write missing rows if any
				if (sb.Length > 0)
				{
					//write line to the .CSV file
					csvwriter.Write(sb.ToString());
					sb.Length = 0;
				}

				//ellapsed time
				var timespan = DateTime.Now - startTime;
				var ellapsed = $"  ellapsed: {timespan}";
				Console.WriteLine(ellapsed);

				//store/update line count
				table.Rows = rowLine;
				table.Pager.Count = pageCount;
				table.Pager.File = pagerName;

				//save last page
				if (table.Generate)
				{
					csvPagerwriter.WriteLine(
									$"{prevGlobalRow}|{prevPosition}|{calculatePageSize(prevPosition, position)}");
				}
				//
				Console.WriteLine($"  ({rowLine}) line(s) processed.");

				//
				if (indexesArray != null)
				{
					//close all index writers, and delete temporary files
					indexesArray.ToList().ForEach(index =>
					{
						index.writer.Dispose();
					});
				}
				if (csvwriter != null)
				{
					csvwriter.Dispose();
				}

			}
			if (!table.Generate)
			{
				Console.WriteLine($"  indexers were not generated!");
			}
			/*
						
 */
		}

		#endregion

		#region Compile Pagers

		public void CompilePagers()
		{
			Console.WriteLine($"\r\nCompiling table pagers:");
			//load __tables.json
			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\r\n[{table.Name}]");
				if (table.Generate)
				{
					//read pager
					var binaryPagerfile = io.Path.Combine(Database.LogPath, $"bin\\{table.Pager.File}");

					var txtPagerfile = io.Path.Combine(Database.LogPath, $"{table.Pager.File}.txt");
					if (!io.File.Exists(txtPagerfile))
					{
						Console.WriteLine($"Cannot find pager data on: {txtPagerfile}");
					}
					else
					{
						//
						using (var writer = new io.BinaryWriter(io.File.Create(binaryPagerfile)))
						{
							var chars = new char[] { '|' };
							foreach (var line in io.File.ReadLines(txtPagerfile)
								.Select(l => l.Split(chars, StringSplitOptions.RemoveEmptyEntries))
								.Select(cols => new
								{
									row = int.Parse(cols[0]),
									position = int.Parse(cols[1]),
									size = int.Parse(cols[2])
								}))
							{
								//page unit
								//page start row
								writer.Write(line.row);
								//page start position
								writer.Write(line.position);
								//page size in bytes
								writer.Write(line.size);
							}
						}
						Console.WriteLine($"   generated binary pager");
					}
				}
				else
				{
					Console.WriteLine($" skipped");
				}
			}
		}

		#endregion

		#region Compile Indexes

		public void CompileIndexes()  // int pageSize = 255
		{
			Console.WriteLine($"\r\nCompiling table indexes:");
			var thisType = this.GetType();
			//
			MethodInfo generateIndexCollection_Method = thisType
				.GetMethod(nameof(generateIndexCollectionKeysMultipleValues), BindingFlags.Instance | BindingFlags.NonPublic);
			//
			MethodInfo compileIndex_Method = thisType
					.GetMethod(nameof(compileIndex), BindingFlags.Instance | BindingFlags.NonPublic);

			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\r\n[{table.Name}]");
				if (table.Generate)
				{
					foreach (var index in table.Columns
						.Where(col => col.Indexed))
					{
						Console.Write($"\r\n  ({index.Name}): {index.Type}, key: {index.Key.IfTrue("[Key]")}");
						//read KeyValuePairs of index

						Type indexType = Type.GetType($"System.{index.Type}");
						//inside .txt is appended
						MethodInfo genericGenerateIndex = generateIndexCollection_Method.MakeGenericMethod(indexType);

						//gets the collection of KeyValue pairs of key,value
						var collection = genericGenerateIndex.Invoke(this,
							new object[]
							{
								index,
								indexType
							});

						//compile index collection
						MethodInfo genericCompileIndex = compileIndex_Method.MakeGenericMethod(indexType);
						//
						var rootPage = genericCompileIndex.Invoke(this,
							new object[] {
								collection,
								Database.PageSize,
								index
						});
						Console.WriteLine($"   compiled.");

						//save index collection to disk
						MethodInfo saveBinaryIndexIndex_Method = thisType
							.GetMethod(nameof(saveBinaryIndex), BindingFlags.Instance | BindingFlags.NonPublic);

						MethodInfo genericSaveBinaryIndex = saveBinaryIndexIndex_Method.MakeGenericMethod(indexType);
						genericSaveBinaryIndex.Invoke(this,
							new object[]
							{
								rootPage,
								index
							});
					}
				}
				else
				{
					//skip pages without the generate:flag
					Console.WriteLine($"   skipped.");
				}

			}
			if (Database.Modified)
			{
				Console.WriteLine($"  updating database structure.");
				Database.Save();
			}
		}

		List<KeyValuePair<T, List<int>>> generateIndexCollectionKeysMultipleValues<T>(
			CsvDbColumn index,
			Type indexType
		)
		{
			//Creating the Type for Generic List.
			Type kp = typeof(KeyValuePair<,>);

			Type[] kpArgs = { indexType, typeof(int) };
			//create generic list type
			Type kpType = kp.MakeGenericType(kpArgs);

			var list = new List<KeyValuePair<T, int>>();

			var listAdd = list.GetType().GetMethod("Add");

			string line;
			//read index data in .TXT file
			var textIndexPath = io.Path.Combine(Database.LogPath, $"{index.Indexer}.txt");
			using (var reader = new io.StreamReader(io.File.OpenRead(textIndexPath)))
			{
				while ((line = reader.ReadLine()) != null)
				{
					var columns = line.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
					//create kp object
					var objvalue = System.Convert.ChangeType(columns[0], indexType);

					var kpObj = Activator.CreateInstance(kpType,
						new object[] {
								System.Convert.ChangeType(columns[0], indexType),
								Int32.Parse(columns[1])
						});
					listAdd.Invoke(list, new object[] { kpObj });
				}
			}

			//return new list ordered by its Key
			list = list.OrderBy(i => i.Key).ToList();
			//save ordered index
			using (var writer =
				new io.StreamWriter(io.File.Create(textIndexPath)))
			{
				foreach (var pair in list)
				{
					writer.WriteLine($"{pair.Key.ToString()}|{pair.Value}");
				}
			}
			//group by key, to see if any duplicates
			//  if any save save as: {index.Indexer}.duplicates.txt

			var groupedList = (from pair in list
												 group pair by pair.Key into g
												 let values = g.Select(p => p.Value).ToList()
												 select new KeyValuePair<T, List<int>>(g.Key, values))
									.ToList();

			//save grouped index for reference
			var duplicatedIndexPath = io.Path.Combine(Database.LogPath, $"{index.Indexer}.duplicates.txt");
			if (groupedList.Any(g => g.Value.Count > 1))
			{
				using (var writer =
				new io.StreamWriter(io.File.Create(duplicatedIndexPath)))
				{
					foreach (var pair in groupedList)
					{
						var values = String.Join(", ", pair.Value);
						writer.WriteLine($"{pair.Key.ToString()}|{values}");
					}
				}
			}
			else
			{
				//ensure if re-compile changed index keep it updated
				if (io.File.Exists(duplicatedIndexPath))
				{
					io.File.Delete(duplicatedIndexPath);
				}
			}
			//
			return groupedList;
		}

		BTreePageBase<T> compileIndex<T>(
			List<KeyValuePair<T, List<int>>> collection,
			int pageSize,
			CsvDbColumn index
		)
					where T : IComparable<T>
		{
			Console.WriteLine($"   processing ({collection.Count}) keys");
			//check table multikey
			var isMultiKey = index.Table.Columns.Count(c => c.Key) > 1;
			if (index.Table.Multikey != isMultiKey)
			{
				Console.WriteLine($"   setting index.Table.Multikey = {isMultiKey};");
				index.Table.Multikey = isMultiKey;
				index.Table.Database.Modified = true;
			}

			//check if key/index are unique
			var duplicated = (from i in collection
												group i by i.Key into g
												let cnt = g.Count()
												where cnt > 1
												select $"{g.Key}: {cnt}").ToList();

			if (duplicated.Count > 0)
			{
				Console.WriteLine($"   duplicated keys: {String.Join(", ", duplicated)}");
			}

			//single-key
			Console.WriteLine($"   index has unique keys");

			//check index.Unique
			var isUnique = duplicated.Count == 0;
			if (index.Unique != isUnique)
			{
				Console.WriteLine($"   updating index.Unique value");
				index.Unique = isUnique;
				index.Table.Database.Modified = true;
			}

			//generate pages
			if (collection.Count <= pageSize)
			{
				Console.WriteLine("   generating one page");
				Console.WriteLine("    -linear algorithm");
			}
			else
			{
				Console.WriteLine("   generating multiple pages, tree structure");
				Console.WriteLine("    -btree algorithm");
			}
			//
			return SplitPages(ref collection, pageSize, 0, collection.Count - 1);
		}

		BTreePageBase<T> SplitPages<T>(
			ref List<KeyValuePair<T, List<int>>> collection,
			int pageSize,
			int start,
			int end
		)
			where T : IComparable<T>
		{
			int count = end - start + 1;
			if (count <= pageSize)
			{
				Console.WriteLine($"{start}...{end} ({count}) -leaf");

				var list = collection.Skip(start).Take(count).ToList();
				return new BTreePageItems<T>(list);
			}
			else
			{
				var center = count / 2;
				Console.WriteLine($"{start}...{center}...{end} ({count}) -node");

				var page = new BTreePageNode<T>(collection[start + center]);

				//get left & right children
				page.Left = SplitPages<T>(ref collection, pageSize, start, start + center - 1);
				page.Left.Parent = page;

				page.Right = SplitPages<T>(ref collection, pageSize, start + center + 1, end);
				page.Right.Parent = page;
				//
				return page;
			}
		}

		void saveBinaryIndex<T>(BTreePageBase<T> rootPage, CsvDbColumn index)
			where T : IComparable<T>
		{
			if (rootPage == null)
			{
				return;
			}
			var pageCount = rootPage.ChildrenCount;
			Console.WriteLine($"   saving ({pageCount}) page(s)");

			//update column page count
			index.PageCount = pageCount;
			index.Table.Database.Modified = true;

			string indexfilepath = io.Path.Combine(Database.BinaryPath, $"{index.Indexer}");

			//page with items .bin
			//this where the main info is
			var indexBinFilePath = indexfilepath + ".bin";
			using (var writer = new io.BinaryWriter(io.File.Create(indexBinFilePath)))
			{
				var collectionPage = getNodeItemPages<T>(rootPage).ToList();

				//MAIN HEADER

				Int32 pageCollectionCount = collectionPage.Count;
				//write amount of pages
				writer.Write(pageCollectionCount);

				//write index key type
				Int32 keyType = (int)index.TypeEnum;
				writer.Write(keyType);

				//pages
				//sizeof: pageCollectionCount, keyType
				var offset = CsvDbGenerator.ItemsPageStart;   // 8;
				foreach (var page in collectionPage)
				{
					page.Offset = offset;
					//save page
					var buffer = page.ToBuffer();
					//
					offset += buffer.Length;
					//
					writer.Write(buffer, 0, buffer.Length);
				}
			}

			//tree page indexer .index
			//this is a light in-memory tree structure for fast search
			using (var writer = new io.BinaryWriter(io.File.Create(indexfilepath)))
			{
				Int32 valueInt32 = 0;
				//write Index main Header
				CsvDbColumnHeader header = new CsvDbColumnHeader();
				//
				header.Value0 = 0;
				//page count
				header.PageCount = pageCount;
				//index.Index
				header.ColumnIndex = index.Index;

				//index.Unique
				//index.Key
				valueInt32 =
					(index.Unique ? Consts.IndexHeaderIsUnique : 0) |
					(index.Key ? Consts.IndexHeaderIsKey : 0) |
					 (rootPage.IsLeaf ? Consts.IndexHeaderIsLeaf : 0);

				//index.Type
				//valueInt32 = valueInt32 << 8;
				valueInt32 |= (byte)index.TypeEnum;
				header.Flags = valueInt32;

				//write Header
				byte[] buffer = header.ToByteArray();
				writer.Write(buffer, 0, buffer.Length);

				//test
				var bin = Convert.ToString(header.Flags, 2);

				//write page tree
				if (!rootPage.IsLeaf)
				{
					StoreTreePage<T>(rootPage, writer);
				}
			}
		}

		IEnumerable<BTreePageItems<T>> getNodeItemPages<T>(BTreePageBase<T> rootPage)
				where T : IComparable<T>
		{
			var stack = new Stack<BTreePageBase<T>>();
			stack.Push(rootPage);
			while (stack.Count > 0)
			{
				var item = stack.Pop();
				if (item.Type == BTreePageTypeEnum.Collection)
				{
					yield return item as BTreePageItems<T>;
				}
				else
				{
					var node = item as BTreePageNode<T>;

					if (node.Right != null)
					{
						stack.Push(node.Right);
					}

					if (node.Left != null)
					{
						stack.Push(node.Left);
					}
				}
			}
		}

		void StoreTreePage<T>(BTreePageBase<T> rootPage, io.BinaryWriter writer)
				where T : IComparable<T>
		{
			if (rootPage.IsLeaf)
			{
				//store id of left
				Int32 valueInt32 = Consts.BTreePageNodeItemsFlag;
				writer.Write(valueInt32);
				//offset
				/////////////////  TO BE USED AS AN ID //////////////////////////
				var leaf = rootPage as BTreePageItems<T>;
				writer.Write(leaf.Offset);
			}
			else
			{
				//store page node
				var buffer = rootPage.ToBuffer();
				writer.Write(buffer, 0, buffer.Length);

				//try to navigate left or right
				var node = rootPage as BTreePageNode<T>;
				int empty = 0;
				if (node.Left == null)
				{
					//signal, no left node
					writer.Write(empty);
					throw new ArgumentException($"Tree Node key:[{node.Root.Key}] has no left child");
				}
				else
				{
					StoreTreePage<T>(node.Left, writer);
				}

				if (node.Right == null)
				{
					//signal, no right node
					writer.Write(empty);
					throw new ArgumentException($"Tree Node key:[{node.Root.Key}] has no right child");
				}
				else
				{
					StoreTreePage<T>(node.Right, writer);
				}
			}
		}

		#endregion

		#region SHOW TREE STRUCTURE OF PAGES  -------DONT UPLOAD TO GITHUB

		public void GenerateTreeStructure()
		{
			Console.WriteLine($"\r\nTree Node Page Structures:");
			foreach (var table in Database.Tables)
			{
				Console.WriteLine($"\r\n[{table.Name}]");
				foreach (var index in table.Columns.Where(c => c.Indexed))
				{
					Console.WriteLine($"\nIndex [{index.Name}]");

					var pathNodeTree = io.Path.Combine(Database.BinaryPath, $"{index.Indexer}");
					var pathItemPages = $"{pathNodeTree}.bin";
					var pathTreeStruct = $"{io.Path.Combine(Database.LogPath, $"{index.Indexer}")}.tree.txt";

					if (!io.File.Exists(pathNodeTree))
					{
						Console.WriteLine("Tree index not found, skip!");
					}
					else
					{
						using (var reader = new io.BinaryReader(io.File.OpenRead(pathNodeTree)))
						using (var writer = new io.StreamWriter(pathTreeStruct))
						{
							writer.WriteLine($"[{table.Name}].{index.Name} Tree Structure");
							writer.WriteLine("");
							writer.WriteLine("Header");
							writer.WriteLine("----------------------");

							//header CsvDbColumnHeader
							var headerBuffer = new byte[CsvDbColumnHeader.Size];
							//
							var read = reader.Read(headerBuffer, 0, headerBuffer.Length);
							var header = CsvDbColumnHeader.FromArray(headerBuffer);

							writer.WriteLine($" Value0: {header.Value0}");
							writer.WriteLine($" Column Index: {header.ColumnIndex}");
							writer.WriteLine($" Page Count: {header.PageCount}");
							writer.WriteLine($" Flags: {header.Flags}");

							var isUnique = (header.Flags & Consts.IndexHeaderIsUnique) != 0;
							var isKey = (header.Flags & Consts.IndexHeaderIsKey) != 0;
							var isLeaf = (header.Flags & Consts.IndexHeaderIsLeaf) != 0;

							byte keyTypeValue = (byte)header.Flags;
							CsvDbColumnTypeEnum keyType = (CsvDbColumnTypeEnum)keyTypeValue;

							writer.WriteLine($"  Unique: {isUnique}");
							writer.WriteLine($"  Key: {isKey}");
							writer.WriteLine($"  Leaf: {isLeaf}");
							writer.WriteLine($"  Key Type: {keyType}");

							if (!isLeaf)
							{
								writer.WriteLine("");
								ReadTreePageShape("", "", reader, keyType, writer);
							}
						}
						Console.WriteLine("Done!");
					}
				}
			}
		}

		void ReadTreePageShape(string keyprefix, string childrenprefix, io.BinaryReader reader,
			CsvDbColumnTypeEnum keyType,
			io.StreamWriter writer)
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
				case Consts.BTreePageNodeFlag:
					var pageSize = reader.ReadInt32();

					var uniqueKeyValue = (flags & Consts.BTreeUniqueKeyValueFlag) != 0;

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
					var key = keyType.LoadKey(reader);

					writer.WriteLine($"{prefix}Key: <{key}> PageSize: {pageSize}, Unique Key Value: {uniqueKeyValue}");

					//left tree page node
					ReadTreePageShape("├──", childrenprefix + "│  ", reader, keyType, writer);

					//right tree page node
					ReadTreePageShape("└──", childrenprefix + "   ", reader, keyType, writer);

					break;
				case Consts.BTreePageNodeItemsFlag:
					//read offset to [table][column].index.bin for page items
					var offset = reader.ReadInt32();
					writer.WriteLine($"{prefix}PageItem, Offset to item page: {offset}");
					break;
				default:
					throw new ArgumentException("Invalid database structure!");
			}
		}

		#endregion

	}
}
