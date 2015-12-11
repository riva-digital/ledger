"""
 db_query
 Riva's database query builder
"""
__author__ = "mdhananjay"

import db_connect


def generate_query_str(select_data=[], from_tables=[], where_and=[], where_or=[], where_and_or=[]):
    """
    This is an all purpose query generator for pretty much any database.
    The format of the tuples in the where_and & the where_or lists should be
    (field, comparison operator, value)
    For example, if in a table you need to search a field called "NAME" whose value is equal to "X",
    the resultant tuple will be ("NAME", "=", "X")
    The where_and_or is a slightly more complicated query. Basically, this mixes both AND & OR
    into the same query, but they are grouped by parenthesis. The format is a list of lists, where
    each child list contains values you want to join using AND, while the items in the main list will
    be joined up using OR. For example, consider the following:
    [[(species, =, snake), (sex, =, 'm')][(species, =, 'bird'), (sex, =, 'f')]]
    This will translate into the following string:
    "(species = 'snake' AND sex = 'm') OR (species = 'bird' AND sex = 'f')"
    :param select_data: A list which contains the fields you want to collect
    :param from_tables: A list of the tables to collect the fields from
    :param where_and: A list of tuples containing all values to be joined up using AND
    :param where_or: A list of tuples containing all values to be joined up using OR
    :param where_and_or: A list of lists, with eac child list containing the tuples for the AND join
    :return: string The Query String, which is like SELECT * FROM * WHERE value is *
    """
    select_str = "SELECT "
    from_str = "FROM "
    where_str = "WHERE "
    query_str = ""

    where_and_stat = 0
    where_or_stat = 0
    where_and_or_stat = 0

    # Before we start, make sure that only one of the following parameters is passed:
    #  where_and
    #  where_or
    #  where_and_or
    if len(where_and):
        where_and_stat = 1

    if len(where_or):
        where_or_stat = 1

    if len(where_and_or):
        where_and_or_stat = 1

    if (where_and_stat and where_or_stat) or (where_and_stat and where_and_or_stat)\
        or (where_or_stat and where_and_or_stat):
        raise ValueError("Multiple conditions found; Please provide for only one type of arguments.")

    if len(select_data) == 0:
        select_str += "* "
    else:
        select_str += ", ".join(select_data)
    select_str += " "

    if len(from_tables) == 0:
        return query_str
    else:
        from_str += ", ".join(from_tables)
    from_str += " "

    query_str = select_str + from_str

    # Now we start the rough part. Basically, WHERE can be used with two keywords, AND & OR
    # AND has a higher precedence than OR, so where stuff is supposed to be intermixed
    # it is better to use (x AND y) OR (a AND b)
    if where_and_or_stat:
        # A little round of checks here, basically, the main list has to have
        # at least 2 child lists, which will define the OR conditions, and
        # within them, each child lists should contain at least two tuples
        # which will define the AND conditions.
        if len(where_and_or) < 2:
            raise ValueError("Insufficient data provided for AND/OR condition.")
        query_str += where_str
        for and_list in where_and_or:
            if len(and_list) < 2:
                raise ValueError("Insufficient data provided for a child list of AND/OR condition.")
            query_str += "("
            for and_cond in and_list:
                query_str += "%s %s %s AND " % (and_cond[0], and_cond[1], and_cond[2])
            query_str = query_str[:-5]
            query_str += ") OR "
        query_str = query_str[:-3]
    elif where_and_stat:
        if len(where_and) == 1:
            query_str += where_str + " ".join(where_and[0])
        else:
            for each_tup in where_and:
                where_str += "%s %s %s AND " % (each_tup[0], each_tup[1], each_tup[2])
            where_str = where_str[:-4]
            query_str += where_str
    elif where_or_stat:
        if len(where_or) == 1:
            query_str += where_str + " ".join(where_and[0])
        else:
            for each_tup in where_or:
                where_str += "%s %s %s OR " % (each_tup[0], each_tup[1], each_tup[2])
            where_str = where_str[:-4]
            query_str += where_str

    return query_str + ";"


def query_db(riva_db_obj, query_str):
    """
    This is going to send the query to the database and return a dict which contains
    the return values from the database.
    :param riva_db_obj: The RivaDatabase object
    :param query_str: The query string to feed to the RivaDatabase
    :return: dict The key-value pairs of the return from the database
    """
    pass


if __name__ == "__main__":
    print generate_query_str(["firstname", "lastname", "empcode"],
                             ["users"],
                             where_and_or=[[("username", "=", "'akulmi'"), ("departmentid", "=", "1")],
                                           [("username", "=", "'mukund.d'"), ("departmentid", "=", "1")]])

