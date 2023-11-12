# Generate a random integer to act as internal ID sequence for data points
from random import shuffle


def map_digit_id(arr, input_field, result_field, give_mapping=False):
    inputs = [obj[input_field] for obj in arr]
    id_arr = list(range(1, len(inputs) + 1))
    shuffle(id_arr)
    mapping = dict(zip(inputs, id_arr))
    for obj in arr:
        obj[result_field] = mapping[obj[input_field]]
    if give_mapping:
        return arr, mapping
    return arr


def generate_digit_id(arr, result_field):
    id_arr = list(range(1, len(arr) + 1))
    shuffle(id_arr)
    for i in range(len(arr)):
        obj = arr[i]
        obj[result_field] = id_arr[i]
    return arr