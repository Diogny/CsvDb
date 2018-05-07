using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CsvDb
{
	/// <summary>
	/// sql query validation contract
	/// </summary>
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

	/// <summary>
	/// column metadata contract
	/// </summary>
	public interface IColumnMeta
	{
		/// <summary>
		/// table name of the column
		/// </summary>
		string TableName { get; }

		/// <summary>
		/// column index in table
		/// </summary>
		int Index { get; }

		/// <summary>
		/// string column type
		/// </summary>
		string Type { get; }
	}

	/// <summary>
	/// column metadata class
	/// </summary>
	public sealed class ColumnMeta : IColumnMeta
	{
		/// <summary>
		/// table name of the column
		/// </summary>
		public string TableName { get; set; }

		/// <summary>
		/// column index in table
		/// </summary>
		public int Index { get; set; }

		/// <summary>
		/// string column type
		/// </summary>
		public string Type { get; set; }
	}

	/// <summary>
	/// represents a default sql query validator against a Csv database
	/// </summary>
	public class CsvDbDefaultValidator : IQueryValidation
	{
		/// <summary>
		/// database
		/// </summary>
		public CsvDb Database { get; }

		/// <summary>
		/// creates a default sql query validator for a Csv database
		/// </summary>
		/// <param name="db">database</param>
		public CsvDbDefaultValidator(CsvDb db)
		{
			if ((Database = db) == null)
			{
				throw new ArgumentException("database undefined for validator");
			}
		}

		/// <summary>
		/// returns a collection of all table names
		/// </summary>
		public IEnumerable<string> TableNames => throw new NotImplementedException();

		/// <summary>
		/// returns true if the database has a table
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <returns></returns>
		public bool HasTable(string tableName) => Database[tableName] != null;

		/// <summary>
		/// returns if database table has a column
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
		public bool TableHasColumn(string tableName, string columnName)
		{
			var table = Database[tableName];
			if (table == null)
			{
				return false;
			}
			return table[columnName] != null;
		}

		/// <summary>
		/// returns a collection of all tables that contain a column
		/// </summary>
		/// <param name="column">column name</param>
		/// <returns></returns>
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

		/// <summary>
		/// returns a collection a all column names in a table
		/// </summary>
		/// <param name="table">table name</param>
		/// <returns></returns>
		public IEnumerable<string> ColumnsOf(string table) => Database[table]?.Columns.Select(c => c.Name);

		/// <summary>
		/// returns the column metadata of a table column
		/// </summary>
		/// <param name="tableName">table name</param>
		/// <param name="columnName">column name</param>
		/// <returns></returns>
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
