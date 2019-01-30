import os
import numpy
import pandas
import InputHandling as IP
import QueryProcessing as QP
import OutputHandling as OP


if __name__ == "__main__":
    path = input("Enter path to data files")
    table_data = IP.read_metadata(path)

    table_list = ["table1", "table2"]
    actual_data = IP.read_data(path, table_list, table_data)
    # print(actual_data)
    # print(table_data)

    while True:
        query_results, query = QP.take_query(table_data, path)
        # OP.show_query_results(query_results, query, path)

