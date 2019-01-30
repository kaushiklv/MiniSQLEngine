def show_query_results(query_results, result_columns, path):
    """
    Write the query results to file
    :param query_results: Query results to the given query
    :param query: Given query
    :param path: Path to all the files given
    """
    with open(path + "/" + "out.txt", 'a') as output_file:
        display_columns = ",".join(result_columns)
        output_file.write(display_columns + '\n')

        for result in query_results:
            result = [str(res) for res in result]
            output_file.write(",".join(result) + '\n')
