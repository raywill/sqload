all:
	g++ main.cpp -lmysqlclient -L ~/mysql-server/build/libmysql/ -I ~/mysql-server/include/ -o sqload

test:
	THREAD_COUNT=10 MYSQL_USER=root MYSQL_PASS='' MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_DB=test ./sqload
