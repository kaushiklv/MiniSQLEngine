import os
import numpy
import pandas
import sys
import InputHandling as IP
import QueryProcessing as QP
import OutputHandling as OP


if __name__ == "__main__":
    query = sys.argv[1]
    table_data = IP.read_metadata()

    query_results, result_columns = QP.take_query(table_data, query)
    OP.show_query_results(query_results, result_columns)

