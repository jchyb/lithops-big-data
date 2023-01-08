"""
Flights to which airports are the least and most expensive?
Counted by finding avg fare of each route and then average of that for each
destination airport.
"""
import lithops
import math

def my_map_function(obj):
    data = obj.data_stream.read()
    airport_values = {} # Data format: {(from, to) avg_price, amount}
    for line in data.splitlines():
        segmented = str(line).split(',')
        startingAirport = segmented[3]
        destinationAirport = segmented[4]
        total_fare = float(segmented[12])
        key = (startingAirport, destinationAirport)
        if key in airport_values.keys():
            new_amount = airport_values[key][1] + 1
            new_sum = airport_values[key][0] + total_fare
            airport_values[key] = (new_sum, new_amount)
        else:
            airport_values[key] = (total_fare, 1)
    return airport_values

def my_reduce_function(results):
    # Aggregate all dictionaries
    airport_values = {}
    for result in results:
        for key, value in result.items():
            if key in airport_values:
                new_sum = airport_values[key][0] + value[0]
                new_amount = airport_values[key][1] + value[1]
                airport_values[key] = (new_sum, new_amount)
            else:
                airport_values[key] = value

    # sum_averages
    avgs = {} # Data format: {dest -> (sum, amount)}
    for key, value in airport_values.items():
        dest = key[1]
        avg = value[0] / value[1]
        if dest in avgs:
            avgs[dest] = (avgs[dest][0] + avg, avgs[dest][1] + 1)
        else:
            avgs[dest] = (avg, 1)

    # Make ordered result list
    unordered_result_list = [(key, value[0]/value[1]) for key, value in avgs.items()]
    result_list = sorted([unordered_result_list], key = lambda l: l[1])
    return result_list

if __name__ == "__main__":
    iterdata = ['s3://lithops-data-jchyb/sm_itineraries.csv']
    object_chunk_size = 4 * 1024 ** 2 # 4 MB

    fexec = lithops.ServerlessExecutor(worker_processes=10)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function,
                    obj_chunk_size=object_chunk_size, map_runtime_memory = 1000, reduce_runtime_memory=1000, timeout = 600)
    result = fexec.get_result()
    print(result)