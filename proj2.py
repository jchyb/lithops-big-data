"""
Which 10 routes are on average the least expensive per one km of travel?
"""
import lithops
import math

min_values_max_amount = 10

def my_map_function(obj):
    data = obj.data_stream.read()
    min_values = []
    for line in data.splitlines():
        segmented = str(line).split(',')
        total_fare = float(segmented[12])
        if not segmented[14].replace('.', '', 1).isdigit():
            continue
        total_travel_distance = float(segmented[14])
        idx = len(min_values) - 1
        metric = total_fare / total_travel_distance
        while idx >= 0 and metric < min_values[idx][0] / min_values[idx][1]:
            idx -= 1
        if idx < min_values_max_amount:
            searchDate = [int(i) for i in segmented[1].split('-')]
            flightDate = [int(i) for i in segmented[2].split('-')]
            startingAirport = segmented[3]
            destinationAirport = segmented[4]
            new_min_value = (total_fare, total_travel_distance, searchDate,
                            flightDate, startingAirport, destinationAirport)
            min_values = min_values[:idx] + [new_min_value] + min_values[idx:-2] 
    return min_values

def my_reduce_function(results):
    min_values = []
    for contender in results:
        new_min = sorted(min_values + contender, key=lambda l: l[0] / l[1])[:min_values_max_amount]
        min_values = new_min
    return min_values

if __name__ == "__main__":
    iterdata = ['s3://lithops-data-jchyb/sm_itineraries.csv']
    object_chunk_size= 4 * 1024 ** 2 # 4 MB

    fexec = lithops.ServerlessExecutor(worker_processes=10)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function,
                    obj_chunk_size=object_chunk_size, map_runtime_memory = 1000, reduce_runtime_memory=1000, timeout = 600)
    result = fexec.get_result()
    print(result)