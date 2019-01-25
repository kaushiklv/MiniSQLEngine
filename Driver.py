import os
import numpy
import pandas
import InputHandling as IP


if __name__ == "__main__":
    path = input("Enter path to data files")
    table_data = IP.read_metadata(path)

    table_list = ["sample", "table1"]
    actual_data = IP.read_data(path, table_list)
    print(table_data)
