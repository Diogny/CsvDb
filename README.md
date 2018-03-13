﻿# CsvDb
A Csv database generator library.


The database schema generated by this library is used in my dev website <a href="http://diogny.com/" target="_blank" title="My PHP website">diogny.com</a>, it powers a custom CSV Database Engine. 

Later I'll include the PHP classes used to read this schema.

#### Schema of [table].[column] index

###### Header

	Column Index
	Page Count
	Flags
	Key Type
	Unique

###### Structure

    Key: <127> PageSize: 16
	├──Key: <64> PageSize: 16
	│  ├──Key: <32> PageSize: 16
	│  │  ├──PageItem, Offse: 8
	│  │  └──PageItem, Offset: 272
	│  └--Key: <96> PageSize: 16
	│     ├──PageItem, Offset: 536
	│     └──PageItem, Offset: 800
	└--Key: <191> PageSize: 16
	   ├──Key: <159> PageSize: 16
	   │  ├──PageItem, Offset: 1056
	   │  └──PageItem, Offset: 1320
	   └──Key: <223> PageSize: 16
		  ├──PageItem, Offset: 1584
		  └──PageItem, Offset: 1848

And an index data with all key and its value(s).

	path\bin\
		__tables.json
		[table].csv
		[table].pager
		[table].[index].index
		[table].[index].index.bin
		
	path\
		[table].[index].index.txt
		[table].[index].index.tree.txt
		[table].[index].index.duplicates.txt
		
		[table].log

To locate a record inside the CSV data, the engine first locate the index, and then search the tree structure for a matching node entry. Then goes to the mapping page where the record is.

This way the engine can locate a csv record super fast. Currently it supports a single key entry value, and a multiple key value. So a key can have multiple values, the index support this.

No need for a database engine like MySQL, Microsoft SQL for an static relatively medium size database. Later I'll show a timechart with response times.

Later I'll add full support the formats JSON, Array of Objects, CSV.
