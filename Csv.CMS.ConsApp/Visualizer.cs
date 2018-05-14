using System;
using System.Text;
using db = CsvDb;
using consts = CsvDb.Consts;
using System.Reflection;
using CsvDb;

namespace Csv.CMS.ConsApp
{

	public sealed class Visualizer
	{

		public db.CsvDb Database { get; set; }

		public Visualizer(db.CsvDb db)
		{
			Database = db;
		}

		#region Tree Structure

		DbColumn GetIndex(string tableColumn)
		{
			DbColumn index = Database.Index(tableColumn);
			if (index == null)
			{
				Console.WriteLine(" index not found, format: [table].index");
			}
			else if (!index.Indexed)
			{
				Console.WriteLine($"[{index.Table.Name}].{index.Name} is not indexed");
				index = null;
			}
			return index;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="tableColumn">table.index</param>
		public void DisplayTreeNodePageStructureInfo(string tableColumn)
		{
			DbColumn index = GetIndex(tableColumn);
			if (index == null)
			{
				return;
			}
			//get type of index class instance
			var indexClassType = index.GetType();
			//get non-public method
			var mthd = indexClassType.GetMethod(nameof(DbColumn.IndexTree)//,
																																		//BindingFlags.Instance | BindingFlags.NonPublic
			);
			//get the type of the index
			var indexType = Type.GetType($"System.{index.Type}");
			//make the method generic
			var geneneric_mthd = mthd.MakeGenericMethod(indexType);
			//call the get geenric index reader method
			var treeIndexer = geneneric_mthd.Invoke(index, new object[] { });

			//call display mthd
			mthd = this.GetType().GetMethod(nameof(Visualizer.DisplayTreeStructureInfo),
				BindingFlags.Instance | BindingFlags.NonPublic);
			geneneric_mthd = mthd.MakeGenericMethod(indexType);

			var output = geneneric_mthd.Invoke(this, new object[]
			{
				treeIndexer
			});
			Console.WriteLine(output);
		}

		public void ShowTableColumnRows<T>(DbColumn column, int count)
				where T : IComparable<T>
		{
			var handler = DbTableDataReader.Create(Database, column.Table.Name);
			var offset = 0;
			//while (count-- > 0)
			{
				var row = handler.ReadDbRecord(offset);

				Console.WriteLine(String.Join(", ", row));
			}

		}

		public void ShowItemPage<T>(DbColumn column, int offset)
			where T : IComparable<T>
		{
			var indexItem = column.IndexItems<T>();
			var page = indexItem[offset];

			if (page == null)
			{
				Console.WriteLine($"Page offset: {offset} is invalid");
				return;
			}
			var handler = DbTableDataReader.Create(Database, column.Table.Name);

			int rows = 0;
			foreach (var item in page.Items)
			{
				Console.WriteLine($"[{item.Key}]  ({item.Value.Count}) offset value(s)");
				//foreach (var ofs in item.Value)
				//{
				//	var row = handler.ReadDbRecord(ofs);
				//	Console.WriteLine($"  {string.Join(", ", row)}");
				//}
				rows++;
			}
			Console.WriteLine($"   displayed ({rows}) key(s)");
		}

		string DisplayTreeStructureInfo<T>(DbIndexTree<T> index)
			where T : IComparable<T>
		{
			var sb = new StringBuilder();
			var nl = Environment.NewLine;
			try
			{

				sb.Append($"[{index.Index.Table.Name}].{index.Index.Name} Tree Structure{nl}");
				sb.Append($"{nl}");
				sb.Append($"Header{nl}");
				sb.Append($"----------------------{nl}");

				sb.Append($" Value0: {index.Header.Value0}{nl}");
				sb.Append($" Column Index: {index.Header.ColumnIndex}{nl}");
				sb.Append($" Page Count: {index.Header.PageCount}{nl}");
				sb.Append($" Flags: {index.Header.Flags}{nl}");

				sb.Append($"  Unique: {index.IsUnique}{nl}");
				sb.Append($"  Key: {index.IsKey}{nl}");
				sb.Append($"  Leaf: {index.IsLeaf}{nl}");
				sb.Append($"  Key Type: {index.KeyType}{nl}");

				if (!index.IsLeaf)
				{
					sb.Append($"{nl}");
					ReadTreePageShape("", "", index.Root, ref sb);
				}
			}
			catch
			{
				sb.Clear();
				sb.Append($"Error displaying index info{nl}");
			}
			return sb.ToString();
		}

		void ReadTreePageShape<T>(string keyprefix, string childrenprefix,
			PageIndexNodeBase<T> root, ref StringBuilder sb)
			where T : IComparable<T>
		{
			//remove last 3 chars if any
			var newCldPref = childrenprefix.Length == 0 ?
						 "" :
						 childrenprefix.Substring(0, childrenprefix.Length - 3);

			var prefix = $"{newCldPref}{keyprefix}";

			var nl = Environment.NewLine;

			switch (root.Type)
			{
				case MetaIndexType.Node:
					var nodePage = root as PageIndexNode<T>;

					sb.Append($"{prefix}Key: <{nodePage.Key}> PageSize: {nodePage.PageSize}, Unique Key Value: {nodePage.UniqueValue}{nl}");

					//left tree page node
					ReadTreePageShape("├──", childrenprefix + "│  ", nodePage.Left, ref sb);

					//right tree page node
					ReadTreePageShape("└──", childrenprefix + "   ", nodePage.Right, ref sb);

					break;
				case MetaIndexType.Items:
					var itemsPage = root as PageIndexItems<T>;
					sb.Append($"{prefix}PageItem, Offset to item page: {itemsPage.Offset}{nl}");
					break;
			}
		}

		#endregion

		#region Page Items

		public void DisplayItemsPageStructureInfo(string tableColumn)
		{
			DbColumn index = GetIndex(tableColumn);
			if (index == null)
			{
				return;
			}
			//get type of index class instance
			var indexClassType = index.GetType();
			//get non-public method
			var mthd = indexClassType.GetMethod(nameof(DbColumn.IndexItems)
			//  BindingFlags.Instance | BindingFlags.NonPublic
			);
			//get the type of the index
			var indexType = Type.GetType($"System.{index.Type}");
			//make the method generic
			var geneneric_mthd = mthd.MakeGenericMethod(indexType);
			//call the get geenric index reader method
			var itemsIndexer = geneneric_mthd.Invoke(index, new object[] { });

			//call display mthd
			mthd = this.GetType().GetMethod(nameof(Visualizer.DisplayItemsPageInfo),
				BindingFlags.Instance | BindingFlags.NonPublic);
			geneneric_mthd = mthd.MakeGenericMethod(indexType);

			var output = geneneric_mthd.Invoke(this, new object[]
			{
				itemsIndexer
			});
			Console.WriteLine(output);
		}



		string DisplayItemsPageInfo<T>(DbIndexItems<T> index)
			where T : IComparable<T>
		{
			var sb = new StringBuilder();
			var nl = Environment.NewLine;
			try
			{
				sb.Append($"[{index.Index.Table.Name}].{index.Index.Name} Items Pages{nl}");
				sb.Append($"{nl}");
				sb.Append($"Header{nl}");
				sb.Append($"----------------------{nl}");

				sb.Append($" PageCount: {index.PageCount}{nl}");
				sb.Append($" Key Type: {index.KeyType}{nl}");
				sb.Append($" Path to Items: {index.PathToItems}{nl}");

				foreach (var page in index.Pages)
				{
					sb.Append($"  Page [{page.Number}]{nl}");
					sb.Append($"   Flags: {page.Flags} | Offset: {page.Offset} | Size: {page.PageSize}{page.UniqueKeyValue.IfTrue(" | [Unique Key]")}");
					sb.Append($"   ({page.ItemsCount}) item(s) | Frequency: {page.Frequency}{nl}");
					//sb.Append($"{nl}");
				}
			}
			catch
			{
				sb.Clear();
				sb.Append($"Error displaying index info{nl}");
			}
			return sb.ToString();
		}

		#endregion

	}

}
