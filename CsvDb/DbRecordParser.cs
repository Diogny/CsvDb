using System;
using System.Collections.Generic;
using System.Text;

namespace CsvDb
{
	/// <summary>
	/// Database csv record parser. Used on DbGenerator.cs
	/// </summary>
	public class DbRecordParser
	{
		public DbTable Table { get; set; }

		public CsvHelper.CsvReader Csv { get; set; }

		/// <summary>
		/// Value without comma if an string
		/// </summary>
		public String[] Values { get; protected internal set; }

		//string wrapped with "" if it has a comma
		String[] CsvValues;

		int Count = 0;

		public DbRecordParser(CsvHelper.CsvReader csv, DbTable table)
		{
			if ((Table = table) == null || (Csv = csv) == null)
			{
				throw new ArgumentException($"Table or Csv reader is null");
			}
			Values = new string[Count = table.Columns.Count];
			CsvValues = new string[Count];
		}

		public bool ReadRecord()
		{
			for (int i = 0, len = Count; i < len; i++)
			{
				var value = Csv.GetField(i);
				Values[i] = value;
				CsvValues[i] = value.ToCsvCol(Table.Columns[i]);
			}
			return true;
		}

		public string Record()
		{
			return String.Join(",", CsvValues);
		}
	}
}
