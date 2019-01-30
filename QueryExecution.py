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
    return "No", "No"


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


def cartesian_product(from_tables, actual_data):
    """
    Take cartesian product of any number of tables given
    :param from_tables: List of tables
    :param actual_data: Table data
    :return: Cartesian product of all the tables
    """
    table1 = actual_data[from_tables[0]]
    table2 = actual_data[from_tables[1]]
    table1_len = len(list(table1.values())[0])
    table2_len = len(list(table2.values())[0])

    result_columns = [table + '.' + col for table in from_tables for col in list(actual_data[table].keys())]

    temp_table1 = []
    for i in range(table1_len):
        temp = []
        for j in table1.keys():
            temp.append(table1[j][i])
        temp_table1.append(temp)

    temp_table2 = []
    for i in range(table2_len):
        temp = []
        for j in table2.keys():
            temp.append(table2[j][i])
        temp_table2.append(temp)

    temp_table = []
    for row1 in temp_table1:
        for row2 in temp_table2:
            temp_table.append(row1 + row2)

    final_temp_table = []
    for table in from_tables[2:]:
        tab = actual_data[table]
        tab_len = len(list(tab.values())[0])

        temp_tab = []
        for i in range(tab_len):
            temp = []
            for j in tab.keys():
                temp.append(tab[j][i])
            temp_tab.append(temp)
        print("0")
        temperary_table = []
        for row1 in temp_table:
            for row2 in temp_tab:
                temperary_table.append(row1 + row2)
        final_temp_table = temperary_table

    return final_temp_table, result_columns


def process_simple_query(from_tables, actual_data, select_columns):
    """
    Take cartesian product of any number of tables given
    :param from_tables: List of tables
    :param actual_data: Table data
    :return: Cartesian product of all the tables
    """
    table1 = actual_data[from_tables[0]]
    table2 = actual_data[from_tables[1]]
    table1_len = len(list(table1.values())[0])
    table2_len = len(list(table2.values())[0])

    result_columns = [table + '.' + col for table in from_tables for col in list(actual_data[table].keys()) if col in select_columns]

    temp_table1 = []
    for i in range(table1_len):
        temp = []
        for j in table1.keys():
            if j in select_columns:
                temp.append(table1[j][i])
        temp_table1.append(temp)

    temp_table2 = []
    for i in range(table2_len):
        temp = []
        for j in table2.keys():
            if j in select_columns:
                temp.append(table2[j][i])
        temp_table2.append(temp)

    temp_table = []
    for row1 in temp_table1:
        for row2 in temp_table2:
            temp_table.append(row1 + row2)

    final_temp_table = []
    for table in from_tables[2:]:
        tab = actual_data[table]
        tab_len = len(list(tab.values())[0])

        temp_tab = []
        for i in range(tab_len):
            temp = []
            for j in tab.keys():
                if j in select_columns:
                    temp.append(tab[j][i])
            temp_tab.append(temp)

        temporary_table = []
        for row1 in temp_table:
            for row2 in temp_tab:
                temporary_table.append(row1 + row2)
        final_temp_table = temporary_table

    return final_temp_table, result_columns


def handle_distinct(query_results, distinct):
    """
    Handles the distinct keyword
    :param query_results:
    :param distinct:
    :return:
    """
    if distinct:
        unique_set = set()
        for row in query_results:
            row = tuple(row)
            unique_set.add(row)
        query_results = unique_set
        return query_results
    else:
        return query_results


def star_query(actual_data, distinct, from_tables):
    """
    Query that gives back the contents of the full table
    :param actual_data: Data taken from the CSV files
    :param from_tables: tables to select the data from
    :return: query results
    """
    if len(from_tables) == 1:
        result_columns = [from_tables[0] + "." + col for col in actual_data[from_tables[0]]]
        query_results = []
        actual_table = actual_data[from_tables[0]]
        table_len = len(list(actual_table.values())[0])

        for i in range(table_len):
            temp = []
            for j, row in enumerate(list(actual_table.keys())):
                temp.append(actual_table[row][i])
            query_results.append(temp)
        query_results = handle_distinct(query_results, distinct)

        return query_results, result_columns
    elif len(from_tables) > 1:
        query_results, result_columns = cartesian_product(from_tables, actual_data)
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns


def simple_query(actual_data, distinct, select_columns, from_tables):
    """
    Simple query with no extra conditions
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param from_tables: tables to select the data from
    :return: query results
    """
    func, column = check_for_aggregate_function(select_columns)
    if func == "No" and len(from_tables) == 1:
        query_results = []
        result_columns = [from_tables[0] + "." + col for col in select_columns]

        table = actual_data[from_tables[0]]
        table_len = len(list(table.values())[0])

        for i in range(table_len):
            temp = []
            for j in select_columns:
                temp.append(table[j][i])
            query_results.append(temp)

        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    elif func == "No" and len(from_tables) > 1:
        query_results, result_columns = process_simple_query(from_tables, actual_data, select_columns)
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    else:
        column_data = []
        if column:
            column_data = actual_data[from_tables[0]][column]
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
    # Star Query
    if len(select_columns) == 1 and select_columns[0] == "*":
        query_results, result_columns = star_query(actual_data, distinct, from_tables)
    # Simple Query
    elif len(from_tables) >= 1 and len(condition_tuples) == 0:
        query_results, result_columns = simple_query(actual_data, distinct, select_columns, from_tables)
    # Simple Query with one or more condition
    elif len(from_tables) == 1 and len(condition_tuples) > 0:
        query_results, result_columns = multi_condition_query(actual_data, distinct, select_columns, condition_tuples)
    # Simple Join query
    elif len(from_tables) == 2 and len(condition_tuples) == 1:
        query_results, result_columns = simple_join_query(actual_data, distinct, select_columns, condition_tuples)
    # Complex Join query with multiple conditions
    elif len(from_tables) == 2 and len(condition_tuples) > 1:
        query_results, result_columns = complex_join_query(actual_data, distinct, select_columns, condition_tuples)
    else:
        query_results = []
    print(query_results)
    return query_results
