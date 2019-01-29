import re


def handle_error_conditions(query):
    """
    Handles all the possible error conditions that can occur in
    a wrong query
    :param query: query which should be checked for mistakes
    :return: Error code

    If Error Code -
    0 - Success
    1 - Error in select statement
    2 - Error in from statement
    3 - Error in where statement
    4 - Incomplete Query
    """
    lower_query = query.lower()
    if "select" not in lower_query or "from" not in lower_query:
        return 4, None
    if ';' in query:
        query = query[:-1]
    query_terms = re.split("select|from|where|SELECT|FROM|WHERE", query)[1:]
    query_terms = [qterm.strip() for qterm in query_terms]
    if not query_terms[0]:
        return 1, None
    elif not query_terms[1]:
        return 2, None
    elif "where" in query:
        if not query_terms[2]:
            return 3, None

    return 0, query_terms


def response_for_error_code(error_code, query):
    """
    Responds correspondingly for each of the different error codes
    :param error_code: Error code for which response must be given
    :return: Appropriate response to the error code
    """
    if error_code == 1:
        print("Check the select statement, try again")
        return 1
    elif error_code == 2:
        print("Check the from statement, try again")
        return 1
    elif error_code == 3:
        print("Check the where statement, try again")
        return 1
    elif error_code == 4:
        print("Incorrect query, try again")
        return 1
    elif query == "exit":
        return 2
    return 0


def check_operator(condition):
    """
    Returns the operator present in the condition
    :param condition: condition
    :return: operator
    """
    if "<=" in condition:
        return "<="
    elif ">=" in condition:
        return ">="
    elif "<" in condition:
        return "<"
    elif ">" in condition:
        return ">"
    elif "=" in condition:
        return "="
    else:
        return ""


def parse_from(query_terms, table_data):
    """
    Parse the from statement and return the table names
    :param query_terms:
    :param table_data:
    :return:
    """
    response = ""
    valid_columns = []
    valid_tables = table_data.keys()
    from_tables = [term.strip() for term in query_terms[1].split(',')]

    for table in from_tables:
        if table not in valid_tables:
            response = table + " is not a Valid Table\n"
            break
        for col in table_data[table]:
            valid_columns.append(col)
    return response, valid_columns, valid_tables, from_tables


def parse_select(query_terms, valid_columns, response):
    """
    Parse the select statement and return the column names
    :param query_terms:
    :param valid_columns:
    :param response:
    :return:
    """
    distinct = 0
    if "distinct" in query_terms[0].lower():
        query_terms[0] = query_terms[0].split()[1]
        distinct = 1

    select_columns = [term.strip() for term in query_terms[0].split(',')]
    allowed = ["max", "min", "avg", "sum", "count"]

    allow = 0
    for col in select_columns:
        for word in allowed:
            if word in col:
                allow = 1
                break
        if allow:
            continue

        if col not in valid_columns:
            response += col + " is not a Valid Column\n"

    for i, col in enumerate(select_columns):
        if "(" in col:
            operation = col[:col.find("(")]
            column = col[col.find("(")+1: col.find(")")]
            select_columns[i] = [operation, column]

    return response, select_columns, distinct


def parse_where(query_terms, table_data, valid_columns, valid_tables, response):
    """
    Parse the where statement and return the condition tuples
    :param query_terms:
    :param table_data:
    :param valid_columns:
    :param valid_tables:
    :param response:
    :return:
    """
    conditions = re.split("AND|OR|and|or", query_terms[2])
    conditions = [cond.strip() for cond in conditions]

    condition_tuples = []
    wrong_condition = 0

    for condition in conditions:
        valid = 1
        condition_tuple = []
        terms = re.split("<|>|>=|<=|=", condition)
        terms = [t.strip() for t in terms]
        if len(terms) == 3:
            del terms[1]
        if "." in terms[0]:
            for term in terms:
                term_parts = term.split(".")
                if term_parts[0] not in valid_tables:
                    response += term_parts[0] + " is not a Valid Table\n"
                    valid = 0
                if term_parts[1] not in valid_columns:
                    response += term_parts[1] + " is not a Valid Column\n"
                    valid = 0
                if term_parts[1] not in table_data[term_parts[0]]:
                    response += term_parts[0] + " doesn't contain column " + term_parts[1] + "\n"
                    valid = 0
                if valid:
                    condition_tuple.append(term_parts)
            if valid:
                condition_tuple.append(check_operator(condition))
        else:
            if terms[0] not in valid_columns:
                response += terms[0] + " is not a Valid Column\n"
            else:
                condition_tuple.append(terms[0])
                condition_tuple.append(terms[1])
                condition_tuple.append(check_operator(condition))
        condition_tuples.append(condition_tuple)

    for condition in condition_tuples:
        if len(condition) == 0:
            wrong_condition = 1

    if wrong_condition:
        print("Where Condition is wrong\n")
    return response, condition_tuples


def parse_and_validate_query(query_terms, table_data):
    """
    Validate the query terms for correctness with the metadata
    :param query_terms: terms which will be the input of the query
    :param table_data: Metadata against which validation will be done
    :return:
    """
    # Check for and get data from the FROM statement
    response, valid_columns, valid_tables, from_tables = \
        parse_from(query_terms, table_data)

    # Check for and get data from the SELECT statement
    response, select_columns, distinct = parse_select(query_terms, valid_columns, response)

    condition_tuples = []
    if len(query_terms) > 2:
        # Check for and get data from the WHERE statement
        response, condition_tuples = \
            parse_where(query_terms, table_data, valid_columns, valid_tables, response)

    return response, select_columns, from_tables, condition_tuples


def process_query(query_terms, table_data):
    """
    Takes a valid query and executes the query
    :param query_terms: terms which will be the input of the query
    :return: query results
    """
    response, select_columns, from_tables, condition_tuples = \
        parse_and_validate_query(query_terms, table_data)
    if response == "":
        response = "Valid"
        print("RESPONSE: ", response)
        print("SELECT ", select_columns)
        print("FROM ", from_tables)
        print("WHERE ", condition_tuples)
    else:
        print("RESPONSE: ", response)
        print("SELECT ", select_columns)
        print("FROM ", from_tables)
        print("WHERE ", condition_tuples)
        return []


def take_query(table_data):
    """
    Take the query as input from the user and then give the results
    of the query back to the user
    :return: query results
    """
    while True:
        query = input("Enter Query: ")
        error_code, query_terms = handle_error_conditions(query)
        error_response = response_for_error_code(error_code, query)

        if error_response == 2:
            break
        elif not error_response:
            if not error_code and query_terms is not None:
                query_results = process_query(query_terms, table_data)

