import generate_file,read_write
import os,time
import threading
from collections import deque

pointer_list = [] # 保存各sub_file文件的读写指针列表
final_result = [] # 保存最终排好序的10000000条记录的数组（实际长度到不了10000000，由另一个线程中定时写入硬盘）
done = False
lock = threading.RLock()

def check_not_empty(content):
    '''判断当前文件内容是否为空'''
    return content !='' and content!='\n'

def close_pointers():
    '''关闭所有的文件指针'''
    global pointer_list
    for pointer in pointer_list:
        pointer.close()

def output_final_list(size):
    '''从各sub_file文件中一次读取一个block并逐条输出到final_result数组中'''
    global pointer_list,final_result,done,lock
    records_list = [] # 用于保存各个sub_file文件的临时数据的数组
    for pointer in pointer_list:
        records = pointer.read(size).decode('utf-8')
        if check_not_empty(records):
            records = read_write.split_records(records)
            records_list.append(records)
    empty = False
    while True:
        if len(records_list)==0:
            done = True
            break

        min_record = [generate_file.RECORDS_NUMBER + 520,-1] # min_record[0] 记录当前最小key，min_record[1]记录对的records_list索引
        for i in range(len(records_list)):
            records = records_list[i]
            if len(records) == 0:
                empty = True
                break
            if records[0][0] < min_record[0]:
                min_record[0] = records[0][0]
                min_record[1] = i
        if empty: # 如果有空数组，需要从对应的文件重新读数据
            pointer = pointer_list[i]
            records = pointer.read(size).decode('utf-8')
            if check_not_empty(records):
                records_list[i] = read_write.split_records(records)
            else: # 若对应的sub_file文件为空，则需要从records_list中将对应的数组去除
                records_list.pop(i)
                pointer.close()
                pointer_list.pop(i)
                
            empty = False
        else:
            record = records_list[min_record[1]].popleft() # 否则将最小的记录插入到final_result数组当中
            lock.acquire()
            try:
                final_result.append(record)
            finally:
                lock.release()
            # final_result.append(record)

def output_final_file():
    global final_result,done,lock
    count = 0
    with open('final.dat','wb') as f:
        while True:
            if done and len(final_result)==0:
                break
            if len(final_result)!=0:
                lock.acquire()
                try:
                    records = ["%s %s\n" % (x[0],x[1]) for x in final_result] # 将tuple数组转换为字符串数组
                    f.write(''.join(records).encode('utf-8'))
                    count += len(final_result)
                    
                    final_result = []
                finally:
                    lock.release()
            time.sleep(10)
    done = False
    print('共写入 %d 条数据' % count)

if __name__ == '__main__':
    path = os.path.join(os.path.abspath('.'),generate_file.FILE_PATH)
    is_exist = False if ((os.path.exists(path) and os.path.isfile(path)) or (not os.path.exists(path))) else True
    assert  is_exist,"the target directory isn't exist."
    print('缓冲区大小: %d byte' % generate_file.BLOCK_SIZE)
    start = time.time()
    t = threading.Thread(target=output_final_file)
    t.start()
    try:
        file_list = os.listdir(path)
        for sub_file in file_list:
            f = open(os.path.join(os.path.abspath(generate_file.FILE_PATH),sub_file),'rb')
            pointer_list.append(f)
        # 一次只读1个BLOCK大小的记录（多出部分舍弃不读）
        size = generate_file.BLOCK_SIZE//generate_file.RECORD_BYTES_LENGTH*generate_file.RECORD_BYTES_LENGTH
        
        output_final_list(size)
        close_pointers()
    finally:
        close_pointers()
    t.join()
    end = time.time()
    print('共耗时:%fs' % (end - start))

    print('缓冲区大小: 1 MB')
    start = time.time()
    t = threading.Thread(target=output_final_file)
    t.start()
    try:
        file_list = os.listdir(path)
        for sub_file in file_list:
            f = open(os.path.join(os.path.abspath(generate_file.FILE_PATH),sub_file),'rb')
            pointer_list.append(f)
        # 一次只读1MB记录（多出部分舍弃不读）
        size = generate_file.CYLINDER_BASED_SIZE//generate_file.RECORD_BYTES_LENGTH*generate_file.RECORD_BYTES_LENGTH
        
        output_final_list(size)
        close_pointers()
    finally:
        close_pointers()
    t.join()
    end = time.time()
    print('共耗时:%fs' % (end - start))