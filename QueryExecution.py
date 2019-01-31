import InputHandling as IP


def check_for_aggregate_function(select_columns):
    """
    Tells if aggregate functions are present in the query
    :param select_columns: columns
    :return: aggregate functions present or not
    """
    aggr_pairs = []
    for col in select_columns:
        if len(col) > 1:
            aggr_pairs.append([col[0], col[1]])
    return aggr_pairs


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
    if temp_table1[0]:
        for i in range(table2_len):
            temp = []
            for j in table2.keys():
                if j in select_columns:
                    temp.append(table2[j][i])
            temp_table2.append(temp)

    temp_table = []
    if temp_table1[0] or temp_table2[0]:
        for row1 in temp_table1:
            if temp_table2[0]:
                for row2 in temp_table2:
                    temp_table.append(row1 + row2)
            else:
                temp_table.append(row1)

    final_temp_table = []
    if len(from_tables) <= 2:
        return temp_table, result_columns
    else:
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


def perform_operation(operand1, operand2, operation):
    """
    :param operand1:
    :param operand2:
    :param operation:
    :return:
    """
    operand1 = int(operand1)
    operand2 = int(operand2)
    if operation == ">":
        return operand1 > operand2
    elif operation == ">=":
        return operand1 >= operand2
    elif operation == "=":
        return operand1 == operand2
    elif operation == "<=":
        return operand1 <= operand2
    elif operation == "<":
        return operand1 < operand2


def star_query(actual_data, distinct, from_tables, condition_tuples, logical_operator):
    """
    Query that gives back the contents of the full table
    :param actual_data: Data taken from the CSV files
    :param distinct: Flag to indicate distinct or not
    :param from_tables: tables to select the data from
    :param condition_tuples: Tuples holding the where conditions
    :return: query results
    """
    if len(from_tables) == 1:
        result_columns = [from_tables[0] + "." + col for col in actual_data[from_tables[0]]]
        query_results = []
        actual_table = actual_data[from_tables[0]]
        table_len = len(list(actual_table.values())[0])

        if len(condition_tuples) == 0:
            for i in range(table_len):
                temp = []
                for j, row in enumerate(list(actual_table.keys())):
                    temp.append(actual_table[row][i])
                query_results.append(temp)
            query_results = handle_distinct(query_results, distinct)
            return query_results, result_columns
        elif len(condition_tuples) == 1:
            valid_rows = []
            for i in range(table_len):
                for j, row in enumerate(list(actual_table.keys())):
                    if row == condition_tuples[0][0] and perform_operation(actual_table[row][i], condition_tuples[0][1], condition_tuples[0][2]):
                        valid_rows.append(i)
            for i in range(table_len):
                temp = []
                if i in valid_rows:
                    for j, row in enumerate(list(actual_table.keys())):
                        temp.append(actual_table[row][i])
                    query_results.append(temp)
            query_results = handle_distinct(query_results, distinct)
            print(query_results)
            return query_results, result_columns
        elif len(condition_tuples) == 2:
            valid_rows = []
            if logical_operator == "and":
                for i in range(table_len):
                    checks = 0
                    for j, row in enumerate(list(actual_table.keys())):
                        if row == condition_tuples[checks][0] and \
                                perform_operation(actual_table[row][i], condition_tuples[checks][1], condition_tuples[checks][2]):
                            checks += 1
                        if checks == 2:
                            valid_rows.append(i)
                            checks = 0
            if logical_operator == "or":
                for i in range(table_len):
                    for j, row in enumerate(list(actual_table.keys())):
                        if (row == condition_tuples[0][0] and perform_operation(actual_table[row][i], condition_tuples[0][1], condition_tuples[0][2])) \
                                or \
                           (row == condition_tuples[1][0] and perform_operation(actual_table[row][i], condition_tuples[1][1], condition_tuples[1][2])):
                            valid_rows.append(i)
            for i in range(table_len):
                temp = []
                if i in valid_rows:
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
    aggr_pairs = check_for_aggregate_function(select_columns)
    if len(aggr_pairs) == 0 and len(from_tables) == 1:
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
    elif len(aggr_pairs) == 0 and len(from_tables) > 1:
        query_results, result_columns = process_simple_query(from_tables, actual_data, select_columns)
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    else:
        aggregate_values = []
        result_columns = []
        for pair in aggr_pairs:
            column_data = []
            for table in from_tables:
                if pair[1] in list(actual_data[table].keys()):
                    column_data = actual_data[table][pair[1]]
            aggregate_values.append(calculate_aggregate_value(pair[0], column_data))
            result_columns.append(pair[0] + "(" + from_tables[0] + '.' + pair[1] + ")")
        return aggregate_values, result_columns


def multi_condition_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator):
    """
    Simple query with one or more conditions
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param from_tables: tables specified in the from statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    aggr_pairs = check_for_aggregate_function(select_columns)
    if len(aggr_pairs) == 0 and len(from_tables) == 1:
        query_results = []
        result_columns = [from_tables[0] + "." + col for col in select_columns]

        table = actual_data[from_tables[0]]
        table_len = len(list(table.values())[0])

        if len(condition_tuples) == 1:
            valid_rows = []
            for i in range(table_len):
                for j, row in enumerate(list(table.keys())):
                    if row == condition_tuples[0][0] and \
                            perform_operation(table[row][i], condition_tuples[0][1], condition_tuples[0][2]):
                        valid_rows.append(i)
            for i in range(table_len):
                temp = []
                if i in valid_rows:
                    for j, row in enumerate(list(table.keys())):
                        temp.append(table[row][i])
                    query_results.append(temp)

            for i in range(table_len):
                temp = []
                for j in select_columns:
                    temp.append(table[j][i])
                query_results.append(temp)
        elif len(condition_tuples) == 2:
            valid_rows = []
            if logical_operator == "and":
                for i in range(table_len):
                    checks = 0
                    for j, row in enumerate(list(table.keys())):
                        if row == condition_tuples[checks][0] and \
                                perform_operation(table[row][i], condition_tuples[checks][1],
                                                  condition_tuples[checks][2]):
                            checks += 1
                        if checks == 2:
                            valid_rows.append(i)
                            checks = 0
            if logical_operator == "or":
                for i in range(table_len):
                    for j, row in enumerate(list(table.keys())):
                        if (row == condition_tuples[0][0] and perform_operation(table[row][i], condition_tuples[0][1], condition_tuples[0][2])) \
                                or \
                           (row == condition_tuples[1][0] and perform_operation(table[row][i], condition_tuples[1][1], condition_tuples[1][2])):
                                valid_rows.append(i)
            for i in range(table_len):
                temp = []
                if i in valid_rows:
                    for row in select_columns:
                        temp.append(table[row][i])
                    query_results.append(temp)
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    else:
        aggregate_values = []
        result_columns = []
        for pair in aggr_pairs:
            column_data = []
            for table in from_tables:
                if pair[1] in list(actual_data[table].keys()):
                    column_data = actual_data[table][pair[1]]
            aggregate_values.append(calculate_aggregate_value(pair[0], column_data))
            result_columns.append(pair[0] + "(" + from_tables[0] + '.' + pair[1] + ")")
        return aggregate_values, result_columns


def simple_join_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator):
    """
    Simple Join query with just one condition i.e. the join condition
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    # TODO: Handle actual join queries for this and multi condition ones
    # TODO: Handling columns when given in . form
    # TODO: Think of all other possible cases and try to cover them
    aggr_pairs = check_for_aggregate_function(select_columns)

    if len(aggr_pairs) == 0 and len(from_tables) > 1:
        query_results, result_columns = process_simple_query(from_tables, actual_data, select_columns)

        select_columns = [col.split(".")[1] for col in result_columns]
        col_map = {i: val for i, val in enumerate(select_columns)}

        if len(condition_tuples) == 1:
            final_results = []
            for row in query_results:
                for i, col in enumerate(row):
                    if col_map[i] == condition_tuples[0][0] and perform_operation(col, condition_tuples[0][1],
                                                                                  condition_tuples[0][2]):
                        final_results.append(row)
            query_results = final_results
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    else:
        aggregate_values = []
        result_columns = []
        for pair in aggr_pairs:
            column_data = []
            for table in from_tables:
                if pair[1] in list(actual_data[table].keys()):
                    column_data = actual_data[table][pair[1]]
            aggregate_values.append(calculate_aggregate_value(pair[0], column_data))
            result_columns.append(pair[0] + "(" + from_tables[0] + '.' + pair[1] + ")")
        return aggregate_values, result_columns


def complex_join_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator):
    """
    Complex Join query with more than one join condition
    :param actual_data: Data taken from the CSV files
    :param distinct: columns are distinct or not
    :param select_columns: columns in the select statement
    :param condition_tuples: conditions on the data
    :return: query results
    """
    aggr_pairs = check_for_aggregate_function(select_columns)

    if len(aggr_pairs) == 0 and len(from_tables) > 1 and len(condition_tuples) == 2:
        query_results, result_columns = process_simple_query(from_tables, actual_data, select_columns)

        col_map = {i: val for i, val in enumerate(select_columns)}

        final_results = []
        for row in query_results:
            checks = 0
            for i, col in enumerate(row):
                if logical_operator == "and":
                    if col_map[i] == condition_tuples[checks][0] and \
                                perform_operation(col, condition_tuples[checks][1], condition_tuples[checks][2]):
                        checks += 1
                    if checks == 2:
                        final_results.append(row)
                        checks = 0
                elif logical_operator == "or":
                    if (col_map[i] == condition_tuples[0][0] and perform_operation(col, condition_tuples[0][1],
                                                                                   condition_tuples[0][2])) \
                            or \
                            (col_map[i] == condition_tuples[1][0] and perform_operation(col, condition_tuples[1][1],
                                                                                        condition_tuples[1][2])):
                        final_results.append(row)
        query_results = final_results
        query_results = handle_distinct(query_results, distinct)
        return query_results, result_columns
    else:
        aggregate_values = []
        result_columns = []
        for pair in aggr_pairs:
            column_data = []
            for table in from_tables:
                if pair[1] in list(actual_data[table].keys()):
                    column_data = actual_data[table][pair[1]]
            aggregate_values.append(calculate_aggregate_value(pair[0], column_data))
            result_columns.append(pair[0] + "(" + from_tables[0] + '.' + pair[1] + ")")
        return aggregate_values, result_columns


def execute_query(path, select_columns, distinct, from_tables, table_data, condition_tuples, logical_operator):
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
        query_results, result_columns = star_query(actual_data, distinct, from_tables, condition_tuples, logical_operator)
    # Simple Query
    elif len(from_tables) >= 1 and len(condition_tuples) == 0:
        query_results, result_columns = simple_query(actual_data, distinct, select_columns, from_tables)
    # Simple Query with one or more condition
    elif len(from_tables) == 1 and len(condition_tuples) > 0:
        query_results, result_columns = multi_condition_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator)
    # Simple Join query
    elif len(from_tables) == 2 and len(condition_tuples) == 1:
        query_results, result_columns = simple_join_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator)
    # Complex Join query with multiple conditions
    elif len(from_tables) == 2 and len(condition_tuples) > 1:
        query_results, result_columns = complex_join_query(actual_data, distinct, select_columns, from_tables, condition_tuples, logical_operator)
    else:
        query_results = []
    print(result_columns)
    print(query_results)
    return query_results, result_columns
