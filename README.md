
# 功能

类似 mysqlslap，都是用于导入压力。

sqload 不同点在于会创建 N 张 partition 表，N 个线程独立往这些表里插入数据。遇到错误时候 sqload 不会停止工作，而是生成下一个 query 继续执行。

写本工具的初衷是得到一个快速生成压力的工具，测试负载均衡。您可以在此基础上继续完善。


# 编译

请在 Makefile 中指定 lmysqlclient 的 lib 和 include 位置，然后指定 make

# 使用

THREAD_COUNT=10 MYSQL_USER=root@obmeter MYSQL_PASS='' MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_DB=test ./sqload
