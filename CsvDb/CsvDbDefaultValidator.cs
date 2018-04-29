using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb
{
	public interface IQueryValidation
	{
		/// <summary>
		/// Returns an enumeration of all table names in the database
		/// </summary>
		IEnumerable<string> TableNames { get; }

		/// <summary>
		/// Returns true if a database has a table
		/// </summary>
		/// <param name="tableName">table name in database</param>
		/// <returns></returns>
		bool HasTable(string tableName);

		/// <summary>
		/// Returns true if a database table has a column
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		bool TableHasColumn(string tableName, string columnName);

		/// <summary>
		/// Returns a collection of all table name of which the column belongs
		/// </summary>
		/// <param name="column">column name</param>
		/// <returns></returns>
		IEnumerable<string> TablesOf(string column);

		/// <summary>
		/// Returns the column names of the table
		/// </summary>
		/// <param name="table">table name</param>
		/// <returns></returns>
		IEnumerable<string> ColumnsOf(string table);

		/// <summary>
		/// Returns the index of a column in a table
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		IColumnMeta ColumnMetadata(string tableName, string columnName);

	}

	public interface IColumnMeta
	{
		string TableName { get; }

		int Index { get; }

		string Type { get; }
	}

	public sealed class ColumnMeta : IColumnMeta
	{
		public string TableName { get; set; }

		public int Index { get; set; }

		public string Type { get; set; }
	}


	public class CsvDbDefaultValidator : IQueryValidation
	{
		public CsvDb Database { get; }

		public IEnumerable<string> TableNames => throw new NotImplementedException();

		public CsvDbDefaultValidator(CsvDb db)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("database undefined for validator");
			}
		}

		public bool HasTable(string tableName) => Database[tableName] != null;

		public bool TableHasColumn(string tableName, string columnName)
		{
			var table = Database[tableName];
			if (table == null)
			{
				return false;
			}
			return table.Column(columnName) != null;
		}

		public IEnumerable<string> TablesOf(string column)
		{
			foreach (var table in Database.Tables)
			{
				if (TableHasColumn(table.Name, column))
				{
					yield return table.Name;
				}
			}
		}

		public IEnumerable<string> ColumnsOf(string table) => Database[table]?.Columns.Select(c => c.Name);

		public IColumnMeta ColumnMetadata(string tableName, string columnName)
		{
			var column = Database.Index(tableName, columnName);
			return column == null ?
				null :
				new ColumnMeta()
				{
					Index = column.Index,
					TableName = column.Table.Name,
					Type = column.Type
				};
		}
	}

}
