import InputHandling as IP


def check_for_aggregate_function(select_columns):
    """
    Tells if aggregate functions are present in the query
    :param select_columns: columns
    :return: aggregate functions present or not
    """
    for col in select_columns:
        if len(col) > 1:
            return col[0], col[1]


def calculate_aggregate_value(func, column):
    """
    Calculate the aggregate value of the data in the column
    :param func: Aggregate function
    :param column: data on which function should be applied
    :return: Aggregate value
    """
    if func == "min":
        return min(column)
    elif func == "max":
        return max(column)
    elif func == "avg":
        return sum(column) / len(column)
    elif func == "sum":
        return sum(column)


def simple_query(actual_data, distinct, select_columns, from_tables):
    """
    Simple query with no extra conditions
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :return: query results
    """
    func, column = check_for_aggregate_function(select_columns)
    column_data = actual_data[from_tables[0]][column]
    if not func:
        pass
    else:
        aggregate_value = calculate_aggregate_value(func, column_data)
        return [aggregate_value]


def multi_condition_query(actual_data, distinct, select_columns, condition_tuples):
    """
    Simple query with one or more conditions
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    return []


def simple_join_query(actual_data, distinct, select_columns, condition_tuples):
    """
    Simple Join query with just one condition i.e. the join condition
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    return []


def complex_join_query(actual_data, distinct, select_columns, condition_tuples):
    """
    Complex Join query with more than one join condition
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    return []


def execute_query(path, select_columns, distinct, from_tables, table_data, condition_tuples):
    """
    Execute the given query and return the results
    :param select_columns: columns in the select statement
    :param distinct: columns are distinct or not
    :param from_tables: tables from which we take the data
    :param table_data: Metadata about all the tables
    :param condition_tuples: conditions on the data
    :return: query results
    """
    actual_data = IP.read_data(path, from_tables, table_data)
    print(actual_data)

    # Handling different types of queries
    # Simple Query
    if len(from_tables) == 1 and len(condition_tuples) == 0:
        query_results = simple_query(actual_data, distinct, select_columns, from_tables)
    # Simple Query with one or more condition
    elif len(from_tables) == 1 and len(condition_tuples) > 0:
        query_results = multi_condition_query(actual_data, distinct, select_columns, condition_tuples)
    # Simple Join query
    elif len(from_tables) == 2 and len(condition_tuples) == 1:
        query_results = simple_join_query(actual_data, distinct, select_columns, condition_tuples)
    # Complex Join query with multiple conditions
    elif len(from_tables) == 2 and len(condition_tuples) > 1:
        query_results = complex_join_query(actual_data, distinct, select_columns, condition_tuples)
    else:
        query_results = []
    print(query_results)
    return query_results
