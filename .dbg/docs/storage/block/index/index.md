Index文件介绍
Index文件是Block目录中的核心文件,是数据查询与检索的关键信息,包括了当前Block中所有series的label数据与chunks数据(sample数据),同时也是结构最复杂的文件,其中包括符号(symbol),序列(series),标签(lable),目录表(TOC)等多个部分组成,此部分由一系列的文章组成,完整分析Index文件的整体格式与其中各个部分的格式细节包括关键数据类型与核心源码

Index文件整体格式
Index文件包含以下部分:
- Symbol Table 记录当前block中所有序列的标签名与标签值
- Series 记录block中包含的所有序列的标签名与标签值在Symbol Table中的索引以及其对应的chunk数据
- LabelIndex
- Posting 记录项(此表项不知道该如何翻译)记录block中包含的所有序列在index文件中Series部分的数据位置偏移
- Lable Offset Table
- Posting Offset Table
- TOC 是目录表(Content of Table)的缩写其中记录了各个部分在index文件中的偏移位置
*核心图形(不包含各部分细节)*

核心数据类型
tsdb.IndexWriter <- index.Writer
核心字段说明
整体写入流程

参考
