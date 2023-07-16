
# 查看指定表
def showTable(connection, table_name):
    
    cursor = connection.cursor()

    sql = f"DESCRIBE {table_name}"
    cursor.execute(sql)
    table_structure = cursor.fetchall()
    for column in table_structure:
        print(column)

    cursor.close()


# 查看指定表中所有数据
def selectAll(connection, table_name):

    cursor = connection.cursor()

    sql = f"select * from {table_name}"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        print(row)

    cursor.close()


# 清空指定表中所有数据
def deleteAll(connection, table_name):

    cursor = connection.cursor()

    sql = f"delete from {table_name}"
    cursor.execute(sql)
    connection.commit()
    cursor.close()