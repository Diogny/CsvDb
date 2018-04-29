using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb
{
	[Flags]
	public enum DbVisualize : int
	{
		None = 0,
		//stops every page
		Paged = 1,
		//for not framed paged show a line under page header
		UnderlineHeader = 2,
		//show a box framed around every page
		Framed = 4,
		//display a column with line numbers
		LineNumbers = 8
	}

	/// <summary>
	/// represents a database visualizer
	/// </summary>
	public class DbVisualizer : IDisposable
	{
		/// <summary>
		/// true if a paged output
		/// </summary>
		public bool Paged { get { return (Options & DbVisualize.Paged) != 0; } }

		/// <summary>
		/// get visualize options
		/// </summary>
		public DbVisualize Options { get; }

		private static int _pagesize = 32;
		/// <summary>
		/// get/set the page size of the visualization
		/// </summary>
		public static int PageSize
		{
			get { return _pagesize; }
			set
			{
				if (value > 1)
				{
					_pagesize = value;
				}
			}
		}

		internal DbRecordReader reader = null;

		/// <summary>
		/// creates a database visualizer, must be disposed to release resources
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="query">query string text</param>
		/// <param name="validator">query validator</param>
		/// <param name="options">visualize options</param>
		public DbVisualizer(CsvDb db, string query, IQueryValidation validator, DbVisualize options = DbVisualize.Paged)
			: this(db, DbQuery.Parse(query, validator), options)
		{ }

		/// <summary>
		/// creates a database visualizer, must be disposed to release resources
		/// </summary>
		/// <param name="db">database</param>
		/// <param name="query">parsed sql query</param>
		/// <param name="options">visualize options</param>
		public DbVisualizer(CsvDb db, DbQuery query, DbVisualize options = DbVisualize.Paged)
		{
			reader = DbRecordReader.Create(db, query);

			Options = options;
		}

		/// <summary>
		/// free reader resources
		/// </summary>
		public void Dispose()
		{
			if (reader != null)
			{
				reader.Dispose();
				reader = null;
			}
		}

		/// <summary>
		/// displays the sql query result
		/// </summary>
		public void Display()
		{
			if (reader == null)
			{
				throw new ArgumentException("database record/row reader is null or undefined");
			}

			if (reader.Query.Columns.IsFunction)
			{
				var row = reader.Rows().ToList();
				var header = row[0][0].ToString();
				var valueStr = row[1][0].ToString();

				Console.WriteLine(header);
				if (Paged && (Options & DbVisualize.UnderlineHeader) != 0)
				{
					Console.WriteLine(new String('─', Math.Max(valueStr.Length, header.Length)));
				}
				Console.WriteLine(valueStr);
			}
			else
			{
				//format anyways with Pagesize for better visibility

				var stop = false;
				var showWait = Paged;
				var headerColumns = reader.Query.Columns.Header;
				//page holder
				var page = new List<List<string>>(PageSize);
				var pageCount = 0;

				var boxed = Paged && (Options & DbVisualize.Framed) != 0;

				var lineNumbers = (Options & DbVisualize.LineNumbers) != 0;

				var enumerator = reader.Rows().GetEnumerator();
				while (!stop)
				{
					//read page
					var count = PageSize;
					page.Clear();

					//format header and column width
					var headerWidths = reader.Query.Columns.Header.Select(h => h.Length).ToList();

					if (lineNumbers)
					{
						//insert minimum of 3 chars for line numbers
						headerWidths.Insert(0, 3);
					}

					while (count-- > 0 && enumerator.MoveNext())
					{
						//convert to string for visualization
						var row = enumerator.Current.Select(c => (c == null) ? String.Empty : c.ToString()).ToList();

						if (lineNumbers)
						{
							row.Insert(0, (reader.RowCount).ToString());
						}

						//add row to page
						page.Add(row);

						//format row columns
						var columnCount = row.Count;
						for (var i = 0; i < columnCount; i++)
						{
							var col = row[i];

							headerWidths[i] = Math.Max(headerWidths[i], (col == null) ? 0 : col.ToString().Length);
						}
					}
					//
					if (!(stop = page.Count == 0))
					{
						var charB = boxed ? '│' : ' ';
						var prefisufix = boxed ? "│" : "";
						var skip = lineNumbers ? 1 : 0;

						//show header
						var header = String.Join(charB,
							reader.Query.Columns.Header.Select((s, ndx) => $" {s.PadRight(headerWidths[ndx + skip])} "));

						if (lineNumbers)
						{
							header = $" {"#".PadRight(headerWidths[0], ' ')} {charB}{header}";
						}

						if (boxed)
						{
							Console.WriteLine($"┌{String.Join('┬', headerWidths.Select(w => new String('─', w + 2)))}┐");
						}

						if (Paged)
						{
							Console.WriteLine($"{prefisufix}{header}{prefisufix}");

							if ((Options & DbVisualize.UnderlineHeader) != 0 && !boxed)
							{
								Console.WriteLine(new String('─', header.Length));
							}
						}

						if (boxed)
						{
							Console.WriteLine($"├{String.Join('┼', headerWidths.Select(w => new String('─', w + 2)))}┤");
						}

						//show columns
						page.ForEach(p =>
						{
							var column = String.Join(charB,
								p.Select((s, ndx) => $" {s.PadRight(headerWidths[ndx])} "));

							Console.WriteLine($"{prefisufix}{column}{prefisufix}");
						});

						if (boxed)
						{
							Console.WriteLine($"└{String.Join('┴', headerWidths.Select(w => new String('─', w + 2)))}┘");
						}

						//if show wait and not first page
						if (showWait && pageCount > 0)
						{
							Console.Write("press any key...");
							var keyCode = Console.ReadKey();
							//remove text
							Console.CursorLeft = 0;
							if (keyCode.Key == ConsoleKey.Escape)
							{
								showWait = false;
							}
						}

						pageCount++;
					}
				}

			}
			Console.WriteLine($" displayed {reader.RowCount.ToString("##,#")} row(s)");
		}
	}

}
