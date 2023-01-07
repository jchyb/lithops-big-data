"""
Simple Lithops example using the map_reduce method.
In this example the map_reduce() method will launch one
map function for each entry in 'iterdata', and then it will
wait locally for the results. Once the results be ready, it
will launch the reduce function.
"""
import lithops

iterdata = [1, 2, 3, 4]


def my_map_function(x):
    return x + 7


def my_reduce_function(results):
    total = 0
    for map_result in results:
        total = total + map_result
    return total


fexec= lithops.FunctionExecutor()
fexec.map_reduce(my_map_function, iterdata, my_reduce_function)
print(fexec.get_result())
                            