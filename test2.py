import lithops

def my_map_function(obj):
    data = obj.data_stream.read()
    count = 0
    for line in data.splitlines():
        count += 1
    return count

def my_reduce_function(results):
    sum_count = 0
    for addition in results:
        sum_count += addition
    return sum_count

if __name__ == "__main__":
    iterdata = ['s3://lithops-data-jchyb/sm_itineraries.csv']
    object_chunk_size= 4 * 1024 ** 2 # 4 Mb

    fexec = lithops.ServerlessExecutor(worker_processes=10)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function,
                    obj_chunk_size=object_chunk_size, map_runtime_memory = 1000)
    result = fexec.get_result()
    print(result)