hive-client
===========

使用hive metastore来描述table的信息，特别是hbase里面的结构信息。


## 目的

HBase 提供了基于字节的信息的存储和获取。但是，应用通常需要比单纯的字节流更具有逻辑意义的数据类型信息，比如int, boolean, string 之类。

所以，应用程序会自己负责设计这些逻辑数据信息和字节流之间的转换方式。这个过程可以被称为“序列化”和“反序列化”。

从HBase里面读取出来的信息，首先需要反序列化以后，才能被其他的系统（比如Drill）处理。

所以，最好有一种通用的方式来描述这些信息，这样，drill就可以通过这些结构的meta，得知如何从HBase获取数据。

## 设计

比如有一个表，逻辑上看是这样的：

···
date                	int                 	              
event0              	string              	                
event1              	string              	                
event2              	string              	                
event3              	string              	                
event4              	string              	                
uhash               	tinyint             	            
uid                 	int                 	            
value               long
timestamp           	bigint              	            
···

这个表，存HBase里面的时候，date event0 event1 event2 event3 event4 uhash uid 序列化为rowkey，value是CF:val, CQ:val 这一列的值，timestamp 是这一列的版本号。

上面这个序列化的例子中，涉及到了两方面的问题：
* 逻辑上的一列序列化的位置？
* 这一列序列化的方式？

HBase有四种地方保存信息：rowkey, Column Qualifier, Column Value, version.

* 比如date, event0 都在rowkey里面保存。这个时候，还需要知道rowkey的格式。
* value 在 column value 里面保存。这个时候，还需要两个信息：CF和CQ信息来指明具体是哪一个ColumnFamily 和 Column。
* timestamp 在version里面保存。这个时候，还需要两个信息：CF和CQ信息来指明具体是哪一个ColumnFamily 和 Column。
* 也可能Column Qualifier就是一列的值。这个时候，还需要一个信息：CF信息来指明具体是哪一个ColumnFamily。

目前来看，至少有两种序列化的方式。主要是针对string以外的类型。
* BINARY方式。比如上述的uid，就序列化为4个字节。
* TEXT方式。比如上述date，就序列化为8个字节长的文本。
* 除了BINARY和TEXT的区别外，有些时候序列化是固定长度的，比如uid（4字节）, date（8字节）；有些时候是不定长的，比如event0。


