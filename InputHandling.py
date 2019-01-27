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


def read_data(path, table_list, table_data):
    """
    Read the actual data of the tables into data structures
    :param path: Takes the directory containing the files as input
    :param table_list: List of tables from which data should be read
    :param table_data: Metadata regarding all the tables of the DB
    :return: returns the data contained in those tables
    """
    csv.register_dialect('myDialect',
                         delimiter=',',
                         quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)

    actual_data_map = {}
    for table in table_list:
        temp = []
        column_names = table_data[table]
        column_map = {}
        with open(table + ".csv", 'r') as f:
            reader = csv.reader(f, dialect='myDialect')
            for row in reader:
                temp.append(row)
        temp = list(map(list, zip(*temp)))
        for i, t in enumerate(temp):
            column_map[column_names[i]] = t
        actual_data_map[table] = column_map

    return actual_data_map
