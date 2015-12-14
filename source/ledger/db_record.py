"""
 db_record
 Riva's database record insert command builder.
 We will NOT be using MODIFY or DELETE records here; we don't want to do any of
 these things in our database. Our database is going to be a purely "ADD/APPEND"
 system.
"""
__author__ = "mdhananjay"


def generate_insert_str(table_name="", param_list=[]):
    """
    Generates the INSERT INTO statement to put data into our database. Takes in a table name,
    and the values for the rows in said table.
    The param_list is a list of dictionaries, which hold the row:value lookup. Eg:
    [{"firstname": "Mukund", "lastname": "Dhananjay"}, {"firstname": "Akesh", "lastname": "Kulmi"}]
    This makes it convenient for the coders to input and verify the data they're sending in
    :param table_name: str Name of the table to insert record into
    :param param_list: list List of dictionaries containing rows and their values
    :return: tuple The INSERT command paired with the list of the tuples containing the row values
    """
    if not table_name:
        raise ValueError("table_name CANNOT BE NULL")

    if not param_list:
        raise ValueError("param_list CANNOT BE EMPTY")

    if type(param_list[0])() != {}:
        raise ValueError("param_list SHOULD CONTAIN ONLY DICTIONARIES")

    # What we have to do here is establish a baseline for the number of rows
    # we will expect. By default the first item found in the param_list will
    # be treated as the "template" object, for the lack of a better term.
    # If the rest of the items in the list have any discrepancies, like missing
    # row names or a different row count, errors will be raised.
    def_row_names = param_list[0].keys()
    def_row_names.sort()
    def_row_count = len(def_row_names)

    insert_str = "INSERT INTO %s VALUES (" % (table_name + " (" + ", ".join(def_row_names) + ")")

    values_list = []
    for p_dict in param_list:
        if len(p_dict.keys()) != def_row_count:
            raise ValueError("Mismatch in row counts. Expected %d, got %d." % (def_row_count, len(p_dict.keys())))

        row_names = p_dict.keys()
        row_names.sort()

        if row_names != def_row_names:
            raise ValueError("Mismatch in the row names. Please check the value passed to param_list.")

        values = []
        for row in def_row_names:
            values.append(p_dict.get(row))
        values_list.append(tuple(values))

    insert_str_list = []
    for each in values_list:
        insert_str_list.append(insert_str + ",".join(each) + ")")

    return insert_str_list

if __name__ == "__main__":
    import db_connect
    import pprint
    insrt = generate_insert_str("department", [{"departmentname": "'script'", "departmentcode": "'scp'"},
                                               {"departmentname": "'concept art'", "departmentcode": "'cat'"},
                                               {"departmentname": "'hair'", "departmentcode": "'har'"}])

    pprint.pprint(insrt)
    rdb = db_connect.RivaDatabase(dbname="riva_users_prototype", dbuser="root")
    rdb.connect()
    rdb.insert_record(insrt)
    rdb.close()

