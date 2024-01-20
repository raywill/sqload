# Geneate CSV Files

CSV 随机数据生成器，支持多种列类型，如: key,rkey,bit,tinyint,smallint,mediumint,int,bigint,float,double,char(20),varchar(10),text,blob,date,datetime,timestamp,time,decimal(10,2),varchar2(10),number(10,2),nchar(3),nvarchar2(3)

其中，key 可以生成严格递增值，rkey 可以生成全局唯一随机值

## Build

```bash
make
```

# Usage

```bash
./generate_csv 10000000 "bigint,int(11),varchar(50),double,date,bigint(20)"
```

- 第一个参数为生成的 csv 文件的记录行数；
- 第二个参数为 csv 每一列的类型，支持几乎所有 MySQL 的类型，以及部分 Oracle 的类型。

# Output Example
```
raywill@raywilllocal csv % ./generate_csv 10 "key,bigint,int(11),varchar(50),double,date,bigint(20)"
0,-4832682865013897366,-1702843776,nfDC0xsYNofDYTjnBnK1XInkNW0zrwZxdQorP1ZQh0M2cSfKHT,-4.23e+08,1960-10-01,6370711650734578995
1,7330547636036405144,-1604609875,FrWH1OQCO9pp36qaLy74SHfk2ZVfzvJFhnQtkVqRXPNTuNWagj,-1.19068e+09,1961-12-24,-5126567758325983711
2,-923188094712707604,1196296991,DTtMc4U55l1xEMQAlwL4QLcAKRCLXpR0vXrKEgajgJXyv13u7d,-1.07472e+09,2020-03-06,-4337529923908177407
3,8379443501853785285,219785586,BZGcbPvOOqb5Jl1GWq3cqhCzC90CAzTLHTn7djqcCIxMnLQxbc,-1.89524e+09,2015-10-24,1077109041905552815
4,-4072237404546206961,119373000,YMT7jZd2TImVqIPRHC2Sd581fHQVjyO1qgWLoMXr2Cqig84dLe,-1.69593e+09,2016-04-11,5364203209828890534
5,-7001010743313544325,-1381474481,8jx9E62X9Xo4TjXJbZ85BrGbGnRCUnhjl1fPwb2Q7rBHzO4XbS,-1.95321e+08,1943-07-21,-4886987591221140823
6,-5546013730098219247,-1157318092,hFAcnnTztUEb0f62dMjVOFPpCmiNPyYeLU4AIpJYXeCrm2jqg5,-8.52185e+08,1911-05-19,-2245757259505334433
7,1119155735697871085,-611596264,PZdZTVyRW5reqoMEjhb3ksCf52C8mDKMB8PPl5CSJnZrZEVjSj,1.28693e+09,1971-10-19,1033062802858446197
8,3434857432393717763,-3264648,Kpy8vXiCk3IFOlMrRfJzRgDeqIYZ0qgksWKsfwrijHHmxUBBPj,1.38785e+09,1908-03-04,-8712969129564195095
9,-993679456530491946,-956925715,dHHHLHLApJUZ0qMwODjWg7PKleYlLOSQiEel6ij4t58IUMxLqW,6.03974e+08,1930-04-01,-7483438534841734302
```

# Performance

生成1千万行，7列，1G左右数据，耗时 19 秒。

```
raywill@raywilllocal csv % time ./generate_csv 10000000 "key,bigint,int(11),varchar(50),double,date,bigint(20)" > /dev/null
18.79s user
0.07s system
99% cpu
18.899 total
```

