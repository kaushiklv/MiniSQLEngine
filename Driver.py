import os
import numpy
import pandas
import InputHandling as IP
import QueryProcessing as QP


if __name__ == "__main__":
    path = input("Enter path to data files")
    table_data = IP.read_metadata(path)

    QP.take_query(table_data)

    table_list = ["table1", "table2"]
    actual_data = IP.read_data(path, table_list, table_data)
    print(actual_data)
    # print(table_data)
