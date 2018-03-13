using System;
using System.Collections.Generic;
using System.Text;

namespace CsvDb
{
	public class CsvDbQuery
	{
		public CsvDb Database { get; protected internal set; }

		public CsvDbQuery(CsvDb db, string query)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("Database is undefined");
			}

			//	SELECT * FROM [table] t
			//		WHERE t.Name == ""

		}

		// *\t[table]\t[[column],==,[value]>]
		//SELECT route_id, rout_short_name FROM routes r
		//		WHERE r.agency_id == "NJT" AND
		//					r.route_type == 2
		// route_id,route_short_name\t[routes]\t[agency_id],==,"NJT"\tAND\t[route_type],==,2

		public void Select(
			CsvDbQueryColSelector selector,
			CsvDbQueryTable table,
			List<CsvDbQueryCondition> where = null)
		{

		}

		public class CsvDbQueryColSelector
		{
			public string Selector { get; set; }

			public List<string> Columns { get; set; }

			public CsvDbQueryColSelector(string selector)
			{
				Columns = new List<string>();
				if (String.IsNullOrWhiteSpace(selector))
				{
					Selector = "*";
				}
				else
				{
					foreach (var c in selector.Split(","))
					{
						if (!String.IsNullOrWhiteSpace(c))
						{
							Columns.Add(c);
						}
					}
					if (Columns.Count == 0)
					{
						Selector = "*";
					}
				}
			}

		}

		public class CsvDbQueryTable
		{
			public string Name { get; set; }

			public string Var { get; set; }

			public CsvDbQueryTable(string table, string var = null)
			{
				Name = table;
				Var = String.IsNullOrWhiteSpace(var) ? "t" : var.ToLower();
			}

		}

		public class CsvDbQueryCondition
		{
			//t.Name
			public string Left { get; set; }

			public string Operator { get; set; }

			public string Right { get; set; }
		}

	}
}
