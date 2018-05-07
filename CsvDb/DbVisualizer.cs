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

		//internal DbRecordReader reader = null;

		internal DbQueryHandler handler = null;

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
			//reader = DbRecordReader.Create(db, query);
			handler = new DbQueryHandler(db, query);

			Options = options;
		}

		/// <summary>
		/// free reader resources
		/// </summary>
		public void Dispose()
		{
			if (handler != null)
			{
				handler.Dispose();
				handler = null;
			}
		}

		/// <summary>
		/// displays the sql query result
		/// </summary>
		public void Display()
		{
			if (handler == null)
			{
				throw new ArgumentException("database record/row reader is null or undefined");
			}

			var boxed = Paged && (Options & DbVisualize.Framed) != 0;

			if (handler.Query.Select.IsFunction)
			{
				var row = handler.Rows().ToList();
				//only one header
				var header = handler.Query.Select.Header[0];

				var valueObj = row[0][0];
				var valueStr = valueObj.ToString();

				int width = Math.Max(header.Length, valueStr.Length);

				if (boxed)
				{
					var spacer = new String('─', width + 2);
					Console.WriteLine($"┌{spacer}┐");
					Console.WriteLine($"│ {header.PadRight(width)} │");
					Console.WriteLine($"├{spacer}┤");
					Console.WriteLine($"│ {valueStr.PadRight(width)} │");
					Console.WriteLine($"└{spacer}┘");
				}
				else
				{
					Console.WriteLine(header);
					if (Paged && (Options & DbVisualize.UnderlineHeader) != 0)
					{
						Console.WriteLine(new String('─', width));
					}
					Console.WriteLine(valueStr);
				}

			}
			else
			{
				//format anyways with Pagesize for better visibility

				var stop = false;
				var showWait = Paged;
				var headerColumns = handler.Query.Select.Header;
				//page holder
				var page = new List<List<string>>(PageSize);
				var pageCount = 0;

				var lineNumbers = (Options & DbVisualize.LineNumbers) != 0;

				var enumerator = handler.Rows().GetEnumerator();
				while (!stop)
				{
					//read page
					var count = PageSize;
					page.Clear();

					//format header and column width
					var headerWidths = handler.Query.Select.Header.Select(h => h.Length).ToList();

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
							row.Insert(0, (handler.RowCount).ToString());
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
							handler.Query.Select.Header.Select((s, ndx) => $" {s.PadRight(headerWidths[ndx + skip])} "));

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
			Console.WriteLine($" displayed {handler.RowCount.ToString("##,#")} row(s)");
		}
	}

}
