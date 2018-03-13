using System;
using System.Collections.Generic;
using System.Text;

namespace CsvDb
{
	public abstract class CsvDbPage
	{
		public Int32 Flags { get; set; }

	}

	public class CsvDbPageNode: CsvDbPage
	{
		//Value or Values

		//Key

		public CsvDbPageNode Left { get; set; }

		public CsvDbPageNode Right { get; set; }

	}

	public class CsvDbPageItems: CsvDbPage
	{

		public int Count { get; set; }


	}

}
