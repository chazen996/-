#!usr/bin/env python3
# -*- coding:utf-8 -*-

from collections import deque
import os,generate_file,time

def split_records(memory): 
    '''将内存中的数据拆分成tuple数组'''
    memory = memory.split('\n')
    records = deque()
    for item in memory:
        if len(item) != 0: # 因为分隔符为\n，拆分后的最后一条记录可能为空
            item = item.split()
            records.append((int(item[0]),item[1])) # 构建tuple存储原数据，方便后续排序
    return records

def check_target_directory():
    '''检查目标文件夹是否存在：
    若目标文件夹不存在，则自动创建；
    若已存在同名文件，则将文件删除后再创建同名文件夹；
    删除目标文件下的的所有文件（需要重新生成）'''
    path = os.path.join(os.path.abspath('.'),generate_file.FILE_PATH)
    if os.path.exists(path) and os.path.isfile(path): # 若存在名为sub_file的文件，需要删除并创建同名文件夹
        os.remove(path)
        os.makedirs(path)
    elif not os.path.exists(path): # 若sub_file文件夹不存在则自动创建
        os.makedirs(path)
    file_list = os.listdir(path)
    for file in file_list:
        os.remove(os.path.join(path,file))

if __name__ == '__main__':
    check_target_directory()
    print('开始处理文件(每次读取50M文件,并进行归并排序)')
    start = time.time()
    with open('file.dat','rb') as f:
        count = 1
        while True:
            memory = f.read(generate_file.MEMORY_SIZE).decode('utf-8') # 一次读取50MB到内存中
            if memory != '' and memory != '\n':
                records = sorted(split_records(memory)) # 使用工具类进行快速排序
                records = ["%s %s\n" % (x[0],x[1]) for x in records] # 将tuple数组转换为字符串数组
                file_name = os.path.join(os.path.abspath(generate_file.FILE_PATH),'sub_file_' + str(count) + '.dat')

                with open(file_name,'wb') as w:
                    w.write(''.join(records).encode('utf-8'))              
                count += 1
            else:
                break
    print('所有文件已生成,总耗时:%fs' % (time.time() - start))