from pymysqlreplication import BinLogStreamReader

mysql_settings = {'host': 'rbrsource', 'port': 3306, 'user': 'yelpdev', 'passwd': ''}

stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=1)

for binlogevent in stream:
    binlogevent.dump()

stream.close()
