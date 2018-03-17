using System;
using System.Collections.Generic;
using System.Text;
using io = System.IO;

namespace CsvDb
{
	public enum CsvDbColumnTypeEnum : byte
	{
		None = 0b000000000,
		Byte = 0b000000010,  // 2
		Int16 = 0b00000100,  // 4
		Int32 = 0b00001000,  // 8
		String = 0b00010000, // 16
		Double = 0b00100000, // 32
	}

	public enum BTreePageTypeEnum
	{
		Node,
		Collection
	}

	public static class Consts
	{
		public const Int32 BTreePageNodeFlag = 0b00000001;

		public const Int32 BTreePageItemsFlag = 0b00000010;

		public const Int32 BTreePageNodeItemsFlag = 0b00000011;


		public const Int32 BTreeUniqueKeyValueFlag = 0b00000100;

		public const Int32 IndexHeaderIsUnique = 0b00100000000;

		public const Int32 IndexHeaderIsKey = 0b01000000000;

		public const Int32 IndexHeaderIsLeaf = 0b10000000000;

	}

	struct IndexStruct
	{
		public CsvDbColumn column;

		public string file;

		public io.StreamWriter writer;
	}

	public interface IBuffered
	{
		byte[] ToByteArray();

	}

	//[StructLayout(LayoutKind.Sequential)]
	public struct CsvDbColumnHeader : IBuffered
	{
		public Int32 Value0;

		public Int32 PageCount;

		public Int32 ColumnIndex;

		public Int32 Flags;

		public static int Size => 16;

		public byte[] ToByteArray()
		{
			var stream = new io.MemoryStream();
			var writer = new io.BinaryWriter(stream);

			writer.Write(this.Value0);
			writer.Write(this.PageCount);
			writer.Write(this.ColumnIndex);
			writer.Write(this.Flags);

			return stream.ToArray();
		}

		public static CsvDbColumnHeader FromArray(byte[] bytes)
		{
			var reader = new io.BinaryReader(new io.MemoryStream(bytes));

			var s = default(CsvDbColumnHeader);

			s.Value0 = reader.ReadInt32();
			s.PageCount = reader.ReadInt32();
			s.ColumnIndex = reader.ReadInt32();
			s.Flags = reader.ReadInt32();

			return s;
		}

	}

	public class CsvDbKeyValues<T>
	{
		public T Key { get; set; }

		public List<int> Values { get; set; }
	}


}
