"""
Find the cheapest ticket.
On which day should it be bought and what is the date of the flight?  
"""
import lithops
import math

def my_map_function(obj):
    data = obj.data_stream.read()
    min_value = (math.inf, None, None, None, None)
    for line in data.splitlines():
        segmented = str(line).split(',')
        total_fare = float(segmented[12])
        if total_fare < min_value[0]:
            searchDate = [int(i) for i in segmented[1].split('-')]
            flightDate = [int(i) for i in segmented[2].split('-')]
            startingAirport = segmented[3]
            destinationAirport = segmented[4]
            min_value = (total_fare, searchDate, flightDate,
                        startingAirport, destinationAirport)
    return min_value

def my_reduce_function(results):
    min_value = (math.inf, 0, 0, 0, 0)
    for contender in results:
        if contender[0] < min_value[0]:
            min_value = contender
    return min_value

if __name__ == "__main__":
    iterdata = ['s3://lithops-data-jchyb/sm_itineraries.csv']
    object_chunk_size= 4 * 1024 ** 2 # 4 MB

    fexec = lithops.ServerlessExecutor(worker_processes=10)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function,
                    obj_chunk_size=object_chunk_size, map_runtime_memory = 1000, timeout = 600)
    result = fexec.get_result()
    print(result)

# segmented = line.split(',')
# searchDate = [int(i) for i in segmented[1].split('-')]
# flightDate = [int(i) for i in segmented[2].split('-')]
# startingAirport = segmented[3]
# destinationAirport = segmented[4]
# economy = bool(segmented[8])
# total_fare = float(segmented[12])