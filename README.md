# disk-access-optimization-experiment
生成一个具有10,000,000个记录的文本文件，其中每个记录由100个字节组成。
实验只考虑记录的一个属性A，假定A为整数类型。
记录在block上封装时，采用non-spanned方式，即块上小于一个记录的空间不使用。
在内存分配50M字节的空间用于外部的merge-sort 

1．生成文本文件，其中属性A的值随机产生。
2．按照属性A进行排序，其中在第二阶段的排序中每个子列表使用一个block大小的缓冲区缓冲数据。
3．按照cylinder-based buffers(1M bytes)的方法，修改第二阶段的算法。
4．比较两种方法的时间性能，如果有更大的内存空间，算法性能还能提高多少？
