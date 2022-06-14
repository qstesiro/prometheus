Symbol介绍
符号表是Index文件中的重要组成部分,其中存储了当前block中所有序列的标签名与标签值,Index文件中其它部分会直接或间接使用符号表,符号表中的所有符号按升序排序

Symbol格式
*图形*
各部分说明

核心数据类型
基本的写入流程
index.Writer.startSymbols
index.Writer.AddSymbol
index.Writer.finishSymbols
index.Symbols中使用的算法

参考
