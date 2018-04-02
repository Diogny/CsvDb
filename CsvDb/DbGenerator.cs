using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using io = System.IO;

namespace CsvDb
{
	/// <summary>
	/// Database generator
	/// </summary>
	public class DbGenerator
	{
		public string ZipFile { get; private set; }

		public CsvDb Database { get; private set; }

		public bool IsBinary { get { return Database.IsBinary; } }

		public string Sufix { get; private set; }

		public static Int32 ItemsPageStart => 8;

		/// <summary>
		/// Creates a database generator for compiling only
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="removeAll">start from scratch, clean database directory</param>
		public DbGenerator(CsvDb db, bool removeAll = true)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException($"Csv Database not provided");
			}
			ZipFile = null;

			Sufix = IsBinary ? ".bin" : String.Empty;

			// remove all but the __tables.json file
			//this's for cleaning, start from scratch

		}

		/// <summary>
		/// Creates a database generator for everything, generate text data and compiling
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="zipfilepath">zZIP file with database data</param>
		/// <param name="removeAll">start from scratch, clean database directory</param>
		public DbGenerator(CsvDb db, string zipfilepath, bool removeAll = true)
			: this(db, removeAll: removeAll)
		{
			if (!io.File.Exists(ZipFile = zipfilepath) || !zipfilepath.EndsWith(".zip"))
			{
				throw new ArgumentException($"ZIP file path doesnot exists: {zipfilepath}");
			}
		}

		#region Schemas

		/// <summary>
		/// Returns the json schema of the system tables
		/// </summary>
		/// <returns></returns>
		public static Newtonsoft.Json.Schema.JSchema Schema()
		{
			var generator = new Newtonsoft.Json.Schema.Generation.JSchemaGenerator();
			var schema = generator.Generate(typeof(DbSchemaConfig));

			return schema;
		}

		#endregion

		#region Text Data Generator

		/// <summary>
		/// Generate database starting data
		/// </summary>
		public void Generate()
		{
			if (String.IsNullOrWhiteSpace(ZipFile))
			{
				throw new ArgumentException("ZIP file not provided");
			}

			var methodName = IsBinary ?
				nameof(DbGenerator.GenerateBinTableText) :
				nameof(DbGenerator.GenerateCsvTableText);

			var zip = io.Compression.ZipFile.OpenRead(ZipFile);
			//generic method
			MethodInfo method =
				this.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);

			foreach (var table in Database.Tables)
			{
				var csvfile = io.Path.Combine(Database.BinaryPath, $"{table.FileName}");

				Console.WriteLine($"\r\n  table: {table.Name}\r\n  processing file ({csvfile})");

				//get zip entry
				var entry = zip.GetEntry(io.Path.ChangeExtension(table.FileName, ".txt"));
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
					MethodInfo genMethod = method.MakeGenericMethod(table.Type);
					//invoke
					genMethod.Invoke(this, new object[] { csv, entry, table, Database.LogPath });
					Console.WriteLine($"  done!");
				}
				catch (Exception ex)
				{
					Console.WriteLine($"error-> {ex.Message}");
				}
			}
			//update system schema
			Database.Save();
			//this's for developing purposes only
			Database.ExportToJson(io.Path.Combine(Database.BinaryPath, "__tables.data.json"));
		}

		CsvHelper.CsvReader OpenCsvFile(io.Compression.ZipArchiveEntry entry)
		{
			using (var outStream = new io.StreamReader(entry.Open()))
			{
				var content = outStream.ReadToEnd();

				byte[] byteArray = Encoding.ASCII.GetBytes(content);

				io.MemoryStream mstream = new io.MemoryStream(byteArray);

				var stream = new io.StreamReader(mstream);

				var csv = new CsvHelper.CsvReader(stream);

				return csv;
			}
		}

		void GenerateBinTableText<TClass>(
			CsvHelper.CsvReader csv,
			io.Compression.ZipArchiveEntry entry,
			DbTable table,
			string rootPath)
		{
			string pagerName = $"{table.Name}.pager";
			string pagerFilename = io.Path.Combine(rootPath, $"{pagerName}.txt");
			var pageCount = 1;
			var pageSize = table.Pager.PagerSize;
			//set to 0 initially, but now starts with "1"
			var globalRow = 1;

			var prevPosition = 0;
			var prevGlobalRow = 0;

			var binaryPosition = 0;
			var stream = new io.MemoryStream();
			var bufferWriter = new io.BinaryWriter(stream);
			var columnTypes = table.ColumnTypes;

			//page size calculator
			Func<int, int, int> calculatePageSize = (prePos, currPos) => currPos - prePos; // + 1;

			using (var csvPagerwriter = new io.StreamWriter(io.File.Create(pagerFilename)))
			{
				IndexStruct[] indexesArray = null;
				//Bin writer
				io.BinaryWriter binWriter = null;

				if (table.Generate)
				{
					//Bin writer
					binWriter = new io.BinaryWriter(io.File.Create(io.Path.Combine(rootPath, $"bin\\{table.Name}.bin")));

					//save column mask
					table.RowMaskLength = Math.DivRem(table.Columns.Count, 8, out int remainder);
					int bits = table.RowMaskLength * 8 + remainder;
					table.RowMask = (UInt64)Math.Pow(2, bits - 1);
					if (remainder != 0)
					{
						table.RowMaskLength++;
					}

					Console.WriteLine($"  creating temporary text index files.");
					//create temporary files for csv pager, and all indexes
					indexesArray = (from col in table.Columns
													where col.Indexed
													let tmpfile = io.Path.Combine(rootPath, $"{col.Indexer}.bin.txt")
													select new IndexStruct
													{
														column = col,
														//csv
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
				Console.WriteLine($"  CSV headers checked.\r\n  Writing Bin database text file, and generating index files...");
				//
				var tableUniqueKeyCol = table.Columns.Where(c => c.Key).First();
				//
				int rowLine = 0;
				var sw = new System.Diagnostics.Stopwatch();
				sw.Start();
				var csvParser = new DbRecordParser(csv, table);

				//read each record of csv entry
				while (csv.Read())
				{
					rowLine++;
					//
					if (table.Generate)
					{
						//get csv record columns string
						csvParser.ReadRecord();

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
								$"{prevGlobalRow}|{prevPosition}|{calculatePageSize(prevPosition, binaryPosition)}");
							//
							prevGlobalRow = globalRow;
							prevPosition = binaryPosition;
						}

						//process indexes
						foreach (var index in indexesArray)
						{
							var indexColValue = csvParser.Values[index.column.Index];

							//position index: $"{value}|{position}"
							//row index:			$"{value}|{globalRow}"
							//if index is key, store its position
							//  otherwise its line number 1-based   must be the key value for single key table.
							string indexBinLine = null;

							if (index.column.Key)
							{
								//for Keys store the position inside the .CSV file
								indexBinLine = $"{indexColValue}|{binaryPosition}";
							}
							else
							{
								//do it too here, so no need to read key tree
								//indexLine = $"{indexColValue}|{tableUniqueKeyValue}";
								indexBinLine = $"{indexColValue}|{binaryPosition}";
							}
							//write line
							index.writer.WriteLine(indexBinLine);
						}

						//save binary data
						UInt64 flags = 0;
						var columnBit = table.RowMask;
						stream.Position = 0;

						foreach (var col in table.Columns)
						{
							var _ndx = col.Index;
							var textValue = csvParser.Values[_ndx];
							//parser get Empty string when should be null
							if (String.IsNullOrEmpty(textValue))
							{
								textValue = null;
							}
							var colType = columnTypes[_ndx];

							if (textValue == null)
							{
								//signal only the null flag as true
								flags |= columnBit;
							}
							else
							{
								var throwException = false;
								switch (colType)
								{
									case DbColumnTypeEnum.Char:
										char charValue = (char)0;
										throwException = !Char.TryParse(textValue, out charValue);
										//write
										bufferWriter.Write(charValue);
										break;
									case DbColumnTypeEnum.Byte:
										byte byteValue = 0;
										throwException = !Byte.TryParse(textValue, out byteValue);
										//write
										bufferWriter.Write(byteValue);
										break;
									case DbColumnTypeEnum.Int16:
										Int16 int16Value = 0;
										throwException = !Int16.TryParse(textValue, out int16Value);
										//write
										bufferWriter.Write(int16Value);
										break;
									case DbColumnTypeEnum.Int32:
										Int32 int32Value = 0;
										throwException = !Int32.TryParse(textValue, out int32Value);
										//write
										bufferWriter.Write(int32Value);
										break;
									case DbColumnTypeEnum.Float:
										float floatValue = 0.0f;
										throwException = !float.TryParse(textValue, out floatValue);
										//write
										bufferWriter.Write(floatValue);
										break;
									case DbColumnTypeEnum.Double:
										Double doubleValue = 0.0;
										throwException = !Double.TryParse(textValue, out doubleValue);
										//write
										bufferWriter.Write(doubleValue);
										break;
									case DbColumnTypeEnum.Decimal:
										Decimal decimalValue = 0;
										throwException = !Decimal.TryParse(textValue, out decimalValue);
										//write
										bufferWriter.Write(decimalValue);
										break;
									case DbColumnTypeEnum.String:
										//write
										bufferWriter.Write(textValue);
										break;
									default:
										throw new ArgumentException($"unsupported type on {col.Name}.{colType} row: {rowLine}");
								}
								if (throwException)
								{
									throw new ArgumentException($"unable to cast: {textValue} on {col.Name}.{colType} row: {rowLine}");
								}
							}
							//shift right column Bit until it reaches 0 -the last column rightmost
							columnBit >>= 1;
						}
						//write true binary record
						var flagsBuffer = BitConverter.GetBytes(flags);
						binWriter.Write(flagsBuffer, 0, table.RowMaskLength);

						//write non-null records
						var recBinary = stream.ToArray();
						binWriter.Write(recBinary, 0, recBinary.Length);

						//update binary position for next row
						binaryPosition += table.RowMaskLength + recBinary.Length;
					}
				};

				//ellapsed time
				sw.Stop();
				Console.WriteLine("ellapsed {0} ms", sw.ElapsedMilliseconds);
				//var timespan = DateTime.Now - startTime;
				//var ellapsed = $"  ellapsed: {timespan}";
				//Console.WriteLine(ellapsed);

				//store/update line count
				table.Rows = rowLine;
				table.Pager.Count = pageCount;
				table.Pager.File = pagerName;

				//save last page
				if (table.Generate)
				{
					csvPagerwriter.WriteLine(
									$"{prevGlobalRow}|{prevPosition}|{calculatePageSize(prevPosition, binaryPosition)}");
				}

				Console.WriteLine($"  ({rowLine}) line(s) processed.");

				//close all index writers, and delete temporary files
				indexesArray?.ToList().ForEach(index =>
				{
					index.writer.Dispose();
				});
				binWriter?.Dispose();
			}
			if (!table.Generate)
			{
				Console.WriteLine($"  indexers were not generated!");
			}
		}

		void GenerateCsvTableText<TClass>(
			CsvHelper.CsvReader csv,
			io.Compression.ZipArchiveEntry entry,
			DbTable table,
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
					//CSV text writer
					csvwriter = new io.StreamWriter(io.File.Create(io.Path.Combine(rootPath, $"bin\\{table.Name}.csv")));

					Console.WriteLine($"  creating temporary text index files.");
					//create temporary files for csv pager, and all indexes
					indexesArray = (from col in table.Columns
													where col.Indexed
													let tmpfile = io.Path.Combine(rootPath, $"{col.Indexer}.txt")
													select new IndexStruct
													{
														column = col,
														//csv
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
				var sw = new System.Diagnostics.Stopwatch();
				sw.Start();
				var csvParser = new DbRecordParser(csv, table);

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

						//write line to temporary CSV buffer
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
							string indexCsvLine = null;

							if (index.column.Key)
							{
								//for Keys store the position inside the .CSV file
								indexCsvLine = $"{indexColValue}|{position}";
							}
							else
							{
								//do it too here, so no need to read key tree
								//indexLine = $"{indexColValue}|{tableUniqueKeyValue}";
								indexCsvLine = $"{indexColValue}|{position}";
							}
							//write line
							index.writer.WriteLine(indexCsvLine);
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
				sw.Stop();
				Console.WriteLine("ellapsed {0} ms", sw.ElapsedMilliseconds);
				//var timespan = DateTime.Now - startTime;
				//var ellapsed = $"  ellapsed: {timespan}";
				//Console.WriteLine(ellapsed);

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

				Console.WriteLine($"  ({rowLine}) line(s) processed.");

				//close all index writers, and delete temporary files
				indexesArray?.ToList().ForEach(index =>
				{
					index.writer.Dispose();
				});
				csvwriter?.Dispose();
			}
			if (!table.Generate)
			{
				Console.WriteLine($"  indexers were not generated!");
			}

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

		/// <summary>
		/// Compile database, generate binary indices
		/// </summary>
		public void Compile()  // int pageSize = 255
		{
			Console.WriteLine($"\r\nCompiling table indexes:");
			var thisType = this.GetType();
			//
			MethodInfo generateIndexCollection_Method = thisType
				.GetMethod(nameof(GenerateIndexCollectionKeysMultipleValues), BindingFlags.Instance | BindingFlags.NonPublic);
			//
			MethodInfo compileIndex_Method = thisType
					.GetMethod(nameof(CompileIndex), BindingFlags.Instance | BindingFlags.NonPublic);

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
								indexType,
								Sufix
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
							.GetMethod(nameof(SaveBinaryIndex), BindingFlags.Instance | BindingFlags.NonPublic);

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

			//update system schema
			Database.Save();
			//this's for developing purposes only
			Database.ExportToJson(io.Path.Combine(Database.BinaryPath, "__tables.compiled.json"));
		}

		List<KeyValuePair<T, List<int>>> GenerateIndexCollectionKeysMultipleValues<T>(
			DbColumn index,
			Type indexType,
			string sufix
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
			var textIndexPath = io.Path.Combine(Database.LogPath, $"{index.Indexer}{sufix}.txt");
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
			var duplicatedIndexPath = io.Path.Combine(Database.LogPath, $"{index.Indexer}{sufix}.duplicates.txt");
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

		BTreePageBase<T> CompileIndex<T>(
			List<KeyValuePair<T, List<int>>> collection,
			int pageSize,
			DbColumn index
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
				//Console.WriteLine($"{start}...{end} ({count}) -leaf");

				var list = collection.Skip(start).Take(count).ToList();
				return new BTreePageItems<T>(list);
			}
			else
			{
				var center = count / 2;
				//Console.WriteLine($"{start}...{center}...{end} ({count}) -node");

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

		void SaveBinaryIndex<T>(BTreePageBase<T> rootPage, DbColumn index)
			where T : IComparable<T>
		{
			if (rootPage == null)
			{
				return;
			}
			var pageCount = rootPage.ChildrenCount;
			Console.WriteLine($"   saving ({pageCount}) page(s)");

			//update column tree page count
			index.PageCount = pageCount;
			index.Table.Database.Modified = true;

			string indexfilepath = io.Path.Combine(Database.BinaryPath, $"{index.Indexer}");

			//page with items .bin
			//this where the main info is
			var indexBinFilePath = indexfilepath + ".bin";
			using (var writer = new io.BinaryWriter(io.File.Create(indexBinFilePath)))
			{
				var collectionPage = GetNodeItemPages<T>(rootPage).ToList();

				//update column item page count
				index.ItemPages = collectionPage.Count;

				//MAIN HEADER

				Int32 pageCollectionCount = collectionPage.Count;
				//write amount of pages
				writer.Write(pageCollectionCount);

				//write index key type
				Int32 keyType = (int)index.TypeEnum;
				writer.Write(keyType);

				//pages
				//sizeof: pageCollectionCount, keyType
				var offset = DbGenerator.ItemsPageStart;   // 8;
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
				PageIndexTreeHeader header = new PageIndexTreeHeader();
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

		IEnumerable<BTreePageItems<T>> GetNodeItemPages<T>(BTreePageBase<T> rootPage)
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

	}
}
