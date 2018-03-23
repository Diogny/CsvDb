using System;
using System.Collections.Generic;
using System.Text;

namespace Csv.CMS.ConsApp.Config
{

	public class AppSettings
	{
		public Window Window { get; set; }
		public Connection Connection { get; set; }
		public Profile Profile { get; set; }
		public Database Database { get; set; }
	}

	public class Database
	{
		public string BasePath { get; set; }
	}

	public class Window
	{
		public int Height { get; set; }
		public int Width { get; set; }
	}

	public class Connection
	{
		public string Value { get; set; }
	}

	public class Profile
	{
		public string Machine { get; set; }
	}

}
