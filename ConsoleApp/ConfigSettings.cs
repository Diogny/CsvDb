using System;
using System.Collections.Generic;
using System.Text;

namespace ConsoleApp.Config
{

	public class ConfigSettings
	{
		public Database Database { get; set; }
	}

	public class Database
	{
		public string BasePath { get; set; }
	}

}
