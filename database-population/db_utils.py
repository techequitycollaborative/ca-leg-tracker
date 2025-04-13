def copy_temp_table(cur, temp_table_name):
    print("Writing table {} to CSV for review...".format(temp_table_name))

    outputquery = """
        COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER
    """

    with open("{0}.csv".format(temp_table_name), "w+") as f:
        cur.copy_expert(outputquery.format(temp_table_name), f)
    return
