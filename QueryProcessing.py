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
    if "select" not in query or "from" not in query:
        return 4, None
    if ';' in query:
        query = query[:-1]
    query_terms = re.split("select|from|where", query)[1:]
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


def process_query(query_terms):
    """
    Takes a valid query and executes the query
    :param query_terms: terms which will be the input of the query
    :return: query results
    """
    pass


def take_query():
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
                query_results = process_query(query_terms)

