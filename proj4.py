"""
How many days before a flight is the average price for a ticket least expensive?
"""
import lithops
import math
from datetime import date

def my_map_function(obj):
    data = obj.data_stream.read()
    # Data format: {(starting_airport, days_before) -> (sum of price, amount)}
    airport_values = {}
    for line in data.splitlines():
        segmented = str(line).split(',')
        starting_airport = segmented[3]
        search_date = [int(i) for i in segmented[1].split('-')]
        flight_date = [int(i) for i in segmented[2].split('-')]
        total_fare = float(segmented[12])
        day_difference = (
            date(flight_date[0], flight_date[1], flight_date[2]) - \
            date(search_date[0], search_date[1], search_date[2])
            ).days
        key = (starting_airport, day_difference)
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

    # Find minimums days for each starting_airport
    mins = {} # {starting_airport -> (days, avg_price)}
    for key, value in airport_values.items():
        starting = key[0]
        days = key[1]
        avg = value[0] / value[1]
        if starting in mins and mins[starting][1] > avg:
            mins[starting] = (days, avg)
        else:
            mins[starting] = (days, avg)

    # Make result list
    result_list = [(key, value[0]) for key, value in mins.items()]
    return result_list

if __name__ == "__main__":
    iterdata = ['s3://lithops-data-jchyb/sm_itineraries.csv']
    object_chunk_size = 4 * 1024 ** 2 # 4 MB

    fexec = lithops.ServerlessExecutor(worker_processes=20)
    fexec.map_reduce(my_map_function, iterdata, my_reduce_function,
                    obj_chunk_size=object_chunk_size, map_runtime_memory = 1000, reduce_runtime_memory=1000, timeout = 600)
    result = fexec.get_result()
    print(result)