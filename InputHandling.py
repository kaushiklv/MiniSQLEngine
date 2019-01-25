import csv


def read_metadata(path):
    """
    Function which will read the metadata from the file
    :param path: Takes the directory containing the files as input
    :return: map of all the table names and corresponding columns
    """
    lines = []
    with open(path + "/" + "metadata.txt", 'r') as meta_file:
        lines = meta_file.readlines()

    lines = [l.strip() for l in lines]

    tables = []
    tail = lines.pop(0)

    while len(lines) > 0:
        table = []
        while "end_table" not in tail:
            if "begin_table" not in tail:
                table.append(tail)
            tail = lines.pop(0)
        tables.append(table)
        if lines:
            tail = lines.pop(0)

    table_map = {}
    for row in tables:
        table_map[row[0]] = row[1:]

    return table_map


def read_data(path, table_list):
    """
    Read the actual data of the tables into data structures
    :param path: Takes the directory containing the files as input
    :param table_list: List of tables from which data should be read
    :return: returns the data contained in those tables
    """
    csv.register_dialect('myDialect',
                         delimiter=',',
                         quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)

    for table in table_list:
        with open(table + ".csv", 'r') as f:
            reader = csv.reader(f, dialect='myDialect')
            for row in reader:
                print(row)