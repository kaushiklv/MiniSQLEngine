import os
import numpy
import pandas
import InputHandling as IP


if __name__ == "__main__":
    path = input("Enter path to data files")
    table_data = IP.read_metadata(path)

    table_list = ["table1", "sample"]
    actual_data = IP.read_data(path, table_list, table_data)
    print(actual_data)
    print(table_data)
