#!usr/bin/env python3
# -*- coding:utf-8 -*-

import random,time

RECORDS_NUMBER = 10000000 # 记录数
RECORD_BYTES_LENGTH = 100 # 每条记录100字节
NUMBER_COUNT = 500000 # 生成1G随机文件时，攒够500000条数据再往硬盘中写入
BLOCK_SIZE = 4096 # 一个BLOCK为4096byte
CYLINDER_BASED_SIZE = 1024*1024 # 缓冲区大小为1MB
MEMORY_SIZE = 50*1024*1024 # 内存大小为50MB
FILE_PATH = 'sub_file' # 分成的若干个子文件路径(./sub_file)
# 用于生成垃圾填充数据的字典表
DICTIONARY = 'tT4xOCTUQqkeq7yJVaqjmhragBPAoE4ESeTNXK1BPG0zwpSCbj7ejLmMJPdnnL1YicM2j28mM9iFCUqeaSgWlAqDLLf5Yzy1HUjx'

def generate_record(): 
    '''生成一条新记录'''
    global RECORDS_NUMBER,RECORD_BYTES_LENGTH,DICTIONARY
    key = str(random.randint(1,RECORDS_NUMBER)) + ' ' # randint会生成[a,b]之间的整数（注意包含两边端点值）
    result = [key,DICTIONARY[0:(RECORD_BYTES_LENGTH-1-len(key))],'\n']
    return ''.join(result)

if __name__ == '__main__':
    count = 0
    print('开始创建包含10000000条记录的文件')
    start = time.time() # 记录生成文件的开始时间
    with open('file.dat','wb') as f:
        while count < RECORDS_NUMBER:
            records = []
            for i in range(0,NUMBER_COUNT): # 一次性生成500000条记录再写入到硬盘中
                record = generate_record()
                assert len(record) == 100 , "the length of record is not suitable!!"
                records.append(record)
            f.write(''.join(records).encode('utf-8')) # 将字节流写入到硬盘
            count += NUMBER_COUNT
    end = time.time() # 记录文件生成的结束时间
    print('创建10000000条记录的时间长度为:%fs\n' % (end - start))