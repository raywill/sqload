// Copyright 1999-2017 Alibaba Inc. All Rights Reserved.
// Author:
//   xiaochu.yh@alipay.com
//
// Multi-thread heavy load sql client for OceanBase 1.4

#include <ctype.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <memory.h>
#include <my_global.h>
#include <mysql.h>
#include <vector>

uint32_t g_rand = 41; // rand seed
int g_varchar_cols = 10; // varchar 大数据的列数
const int g_varchar_width = 10240; // varchar 大数据每个 cell 的宽度
int g_thread_cnt = 0; // 总线程数
uint64_t *g_total_query = NULL;
uint64_t *g_fail_query = NULL; // 统计失败次数，数组，每个线程一项
uint64_t *g_max_query = NULL; // 预计最多Query数，数组，每个线程一项
int g_last_error_no = 0; // 最后一次错误
int g_thread_rows = 2000000; // 每个线程最多负责插入多少行数据
const char *g_tb_prefix = "t_"; // 建立的表前缀
typedef enum {
  BALANCED_MODE = 0,
  IMBALANCED_MODE = 1
} RunMode;
RunMode g_mode = IMBALANCED_MODE; // 运行模式，0 = 均匀插入数据到所有 partition，1 = 只插入到某个 unit
//const RunMode g_mode = BALANCED_MODE; // 运行模式，0 = 均匀插入数据到所有 partition，1 = 只插入到某个 unit
int *g_bias_partition_id;

void *freeze_runner(void *arg)
{
  int ret = 0;

  printf("freeze thread running\n");

  uint64_t last_total_query = 0;
  uint64_t last_fail_query = 0;

  int idle_times = 0;

  while (1) {
    uint64_t cur_max_query = 0;
    uint64_t cur_total_query = 0;
    uint64_t cur_fail_query = 0;

    for (int i = 0; i < g_thread_cnt; ++i) {
      cur_fail_query += g_fail_query[i];
      cur_total_query += g_total_query[i];
      cur_max_query += g_max_query[i];
    }

    if (cur_total_query - last_total_query > 10000) {
      fprintf(stderr, "PROGRESS:  succ/fail/total/max = %ld/%ld/%ld/%ld. about %lf GB written\n",
              cur_total_query - cur_fail_query, cur_fail_query, cur_total_query, cur_max_query,
              1.0 * (cur_total_query - cur_fail_query) * g_varchar_cols * g_varchar_width / 1024 / 1024 /1024);
    }

    if (cur_fail_query - last_fail_query > 1000) {
      fprintf(stderr, "FAIL WARNING. succ/fail/total/max = %ld/%ld/%ld/%ld. last_errno=%d\n",
              cur_total_query - cur_fail_query, cur_fail_query, cur_total_query, cur_max_query, g_last_error_no);
    }

    if (cur_total_query - last_total_query > 10000 || cur_fail_query - last_fail_query > 1000) {
      last_total_query = cur_total_query;
      last_fail_query = cur_fail_query;
    }

    usleep(10 * 1000 * 1000); // 10 second


    for (int i = 0; i < g_thread_cnt; ++i) {
      cur_fail_query += g_fail_query[i];
      cur_total_query += g_total_query[i];
    }
    if (last_total_query == cur_total_query) {
      idle_times++;
      if (idle_times > 10) { // 100s 没有任何query执行，退出
        pthread_exit(NULL);
      }
    }
  }

  pthread_exit(NULL);
}

void build_varchar(char *buf, int len)
{
  int i = 0;
  for (i = 0; i < len - 100; ++i) {
    g_rand *= 0x9e3779b1;
    uint32_t r = g_rand;
    buf[i] = 'a' + r % 26;
  }
  for (; i < len - 1; ++i) {
    buf[i] = '*';
  }
  buf[len - 1] = '\0';
}

int gen_pk(int thread_id, std::vector<int> &pks)
{
  int ret = 0;
  const int buf_len = 10240;
  char *sql_buf = new char[buf_len];
  int pos = 0;
  if (g_mode == BALANCED_MODE) {
    pos = sprintf(sql_buf,
            "SELECT /*+ READ_CONSISTENCY(WEAK) */ distinct partition_id FROM __all_meta_table WHERE table_id = (SELECT table_id FROM __all_table WHERE table_name = '%s%d' ORDER BY table_id DESC limit 1)",
            g_tb_prefix, thread_id);
  } else {
    pos = sprintf(sql_buf,
            "SELECT /*+ READ_CONSISTENCY(WEAK) */ distinct partition_id FROM __all_meta_table WHERE table_id = (SELECT table_id FROM __all_table WHERE table_name = '%s%d' ORDER BY table_id DESC limit 1) AND unit_id = (SELECT unit_id FROM __all_unit WHERE unit_id > 1000 LIMIT 1)",
            g_tb_prefix, thread_id);
  }

  char *host = getenv("MYSQL_HOST");
  char *port = getenv("MYSQL_PORT");
  MYSQL *conn;
  conn = mysql_init(NULL);
  // location aware
  if (0 == ret) {
    if (mysql_real_connect(conn, host, "root", "", "oceanbase", atoi(port), NULL, 0) == NULL) {
      printf("Error-7 %u: %s\n", mysql_errno(conn), mysql_error(conn));
    }
  }
  if (0 == ret) {
    if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
      fprintf(stderr, "fail execute %s. ret=%d\n", sql_buf, ret);
    } else {
      MYSQL_RES *res;
      MYSQL_ROW row;
      res = mysql_use_result(conn);
#ifdef PRINT_DETAIL
      printf("thread %d pk list:", thread_id);
#endif
      while ((row = mysql_fetch_row(res)) != NULL) {
        pks.push_back(atoi(row[0]));
#ifdef PRINT_DETAIL
        printf("%d,", atoi(row[0]));
#endif
      }
#ifdef PRINT_DETAIL
      printf("\n");
#endif
      mysql_free_result(res);
    }
  }
  if (NULL != conn) {
    mysql_close(conn);
  }
  if (pks.size() <= 0) {
#ifdef PRINT_DETAIL
    fprintf(stderr, "no partition id found!!!\n");
#endif
  }
  if (sql_buf) {
    delete sql_buf;
  }
  return ret;
}


// 每 2 个 table 一个 table group
int build_tablegroup(char *sql_buf, int buf_len, int thread_id)
{
  int pos = 0;
  pos = sprintf(sql_buf, "CREATE TABLEGROUP IF NOT EXISTS sqload_tg%d", thread_id / 2);
  return pos;
}

int build_create_table(char *sql_buf, int buf_len, int cols, int thread_id)
{
  int pos = 0;
  int partition_count = 1;
  pos = sprintf(sql_buf, "CREATE TABLE IF NOT EXISTS %s%d("
          "primary key(pk1, pk2, pk3, pk4, pk5),"
          "pk1 int,"
          "pk2 int,"
          "pk3 int,"
          "pk4 int,"
          "pk5 int,", g_tb_prefix, thread_id);
  for (int i = 1; i < cols; ++i) {
    pos += sprintf(sql_buf + pos, "c%d varchar(%d),", i, g_varchar_width);
  }
  pos += sprintf(sql_buf + pos, "c%d varchar(%d))", cols, g_varchar_width);
  pos += sprintf(sql_buf + pos, " TABLEGROUP = sqload_tg%d", thread_id / 2); // 1/3 tg0, 2/3 tg1
  pos += sprintf(sql_buf + pos, " PARTITION BY KEY(pk1) PARTITIONS %d", partition_count);
  return pos;
}


int build_insert(char *buf, int buf_len, int cols, int thread_id, std::vector<int> pks)
{
  int scale = 100; // 用于选择 SCAN 数据范围
  int pos = 0;
  char randchar[g_varchar_width];

  pos = sprintf(buf, "INSERT INTO %s%d VALUES(", g_tb_prefix, thread_id);
  int i = 0;

  int pk = 0;
  pk = (pks.size() > 0) ? pks[(random() % pks.size())] : random() % scale;
  pos += sprintf(buf + pos, "%d,", pk); // i = 0
  pk = (random() % scale);
  pos += sprintf(buf + pos, "%d,", pk); // i = 1
  for (i = 2; i < 5; ++i) {
    pos += sprintf(buf + pos, "%d,", random());
  }

  int width = g_varchar_width;
  for (int i = 1; i < cols; ++i) {
    build_varchar(randchar, width);
    pos += sprintf(buf + pos, "'%s',", randchar);
  }

  build_varchar(randchar, width);
  pos += sprintf(buf + pos, "'%s')", randchar);
  return pos;
}

int make_load(MYSQL *conn, int thread_id)
{
  int ret = 0;
  // LOAD 特点：
  // 写入大批量数据，定时自动做 major freeze，统计执行失败次数，失败次数每上升 1
  // 个点打印一次执行统计，每执行 1000 个 query 打印一次执行统计。
  const int buf_len = g_varchar_cols * g_varchar_width + 4096;
  char *sql_buf = new char[buf_len];
  std::vector<int> pks;
  if (0 != (ret = gen_pk(thread_id, pks))) {
    printf("Error-8 %u: %s\n", mysql_errno(conn), mysql_error(conn));
  }
  if (0 == ret && pks.size() > 0) {
    int rows = random() % g_thread_rows;
    g_max_query[thread_id] = rows;
    printf("thread %d will insert %d rows\n", thread_id, rows);
    for (int row = 0; row < rows; ++row) {
      int pos = build_insert(sql_buf, buf_len, g_varchar_cols, thread_id, pks);
      // printf("%s\n", sql_buf);
      if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
        g_fail_query[thread_id]++;
        g_last_error_no = mysql_errno(conn);
      }
      g_total_query[thread_id]++;
    }
  } else {
    printf("INFO: no data in first unit, skip for imbalance-mode. thread_id=%d\n", thread_id);
  }
  if (sql_buf) {
    delete sql_buf;
  }
  return ret;
}



void *task_runner(void *arg)
{
  int ret = 0;
  int thread_id = *((int *)arg);
  char *user = getenv("MYSQL_USER");
  char *pass = getenv("MYSQL_PASS");
  char *host = getenv("MYSQL_HOST");
  char *port = getenv("MYSQL_PORT");
  char *db   = getenv("MYSQL_DB");

#ifdef PRINT_DETAIL
  printf("task %d running\n", thread_id);
#endif

  if (0 == ret) {
    MYSQL *conn;
    mysql_thread_init();
    conn = mysql_init(NULL);
    if (conn == NULL) {
      printf("Error-1 %u: %s\n", mysql_errno(conn), mysql_error(conn));
    } else if (mysql_real_connect(conn, host, user, pass, db, atoi(port), NULL, 0) == NULL) {
      printf("Error-2 %u: %s\n", mysql_errno(conn), mysql_error(conn));
    } else if (0 != (ret = make_load(conn, thread_id))) {
      printf("Error-3 %u: %s\n", mysql_errno(conn), mysql_error(conn));
    }
    if (NULL != conn) {
      mysql_close(conn);
    }
    mysql_thread_end();
  }

  pthread_exit(NULL);
}

int create_db(MYSQL *conn, int thread_id)
{
  int ret = 0;
  // LOAD 特点：
  // 写入大批量数据，定时自动做 major freeze，统计执行失败次数，失败次数每上升 1
  // 个点打印一次执行统计，每执行 1000 个 query 打印一次执行统计。
  const int buf_len = g_varchar_cols * 1024 + 4096;
  char *sql_buf = new char[buf_len];
  int pos = 0;
  if (0 == ret) {
    pos = build_tablegroup(sql_buf, buf_len, thread_id);
    if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
      fprintf(stderr, "fail create table %s%d. ret=%d\n", g_tb_prefix, thread_id, ret);
    } else {
#ifdef PRINT_DETAIL
      printf("%s\n", sql_buf);
#endif
    }
  }
  if (0 == ret) {
    pos = build_create_table(sql_buf, buf_len, g_varchar_cols, thread_id);
    if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
      fprintf(stderr, "fail create table %s%d. ret=%d\n", g_tb_prefix, thread_id, ret);
    } else {
#ifdef PRINT_DETAIL
      printf("%s\n", sql_buf);
#endif
    }
  }
  if (sql_buf) {
    delete sql_buf;
  }
  return ret;
}

int init_db()
{
  int ret = 0;
  int thread_id = 0;
  char *user = getenv("MYSQL_USER");
  char *pass = getenv("MYSQL_PASS");
  char *host = getenv("MYSQL_HOST");
  char *port = getenv("MYSQL_PORT");
  char *db   = getenv("MYSQL_DB");

  printf("init db\n");

  MYSQL *conn;
  mysql_thread_init();
  conn = mysql_init(NULL);
  if (conn == NULL) {
    printf("Error-4 %u: %s\n", mysql_errno(conn), mysql_error(conn));
  } else if (mysql_real_connect(conn, host, user, pass, db, atoi(port), NULL, 0) == NULL) {
    printf("Error-5 %u: %s\n", mysql_errno(conn), mysql_error(conn));
  } else {
    for (thread_id = 0; thread_id < g_thread_cnt && 0 == ret; ++thread_id) {
      if (0 != (ret = create_db(conn, thread_id))) {
        printf("Error-6 %u: %s\n", mysql_errno(conn), mysql_error(conn));
      }
    }
  }
  if (NULL != conn) {
    mysql_close(conn);
  }
  mysql_thread_end();
  return ret;
}

void init()
{
  srandom(time(NULL));
}


int main(int argc, char **argv)
{
  int ret = 0;
  char *thread = getenv("THREAD_COUNT");

  char *user = getenv("MYSQL_USER");
  char *pass = getenv("MYSQL_PASS");
  char *host = getenv("MYSQL_HOST");
  char *port = getenv("MYSQL_PORT");
  char *db   = getenv("MYSQL_DB");

  if (NULL == user
      || NULL == pass
      || NULL == host
      || NULL == port
      || NULL == db) {
    ret = -1;
    fprintf(stderr, "invalid env param supplied\n");
  }

  char *mode = getenv("MODE");
  if (NULL != mode) {
    if (0 == strcasecmp("imbalance", mode)) {
      g_mode = IMBALANCED_MODE;
      printf("g_mode:IMBALANCED_MODE\n");
    } else {
      g_mode = BALANCED_MODE;
      printf("g_mode:BALANCED_MODE\n");
    }
  }

  if (NULL == thread) {
    g_thread_cnt = 1; // default
    printf("note: provide THREAD_COUNT to enable multithread test\n");
  } else {
    g_thread_cnt = atoi(thread);
    printf("g_thread_cnt:%d\n", g_thread_cnt);
  }

  char *max_row_per_thread = getenv("MAX_ROW");
  if (NULL != max_row_per_thread) {
    g_thread_rows = atoll(max_row_per_thread);
    printf("g_thread_rows:%ld\n", g_thread_rows);
  }

  init();

  if (0 == ret) {
    if (0 != (ret = mysql_library_init(0, NULL, NULL))) {
      fprintf(stderr, "could not initialize MySQL library. ret=%d\n", ret);
    } else {
      pthread_t *thread_id = new pthread_t[g_thread_cnt];
      g_total_query = new uint64_t[g_thread_cnt];
      g_fail_query = new uint64_t[g_thread_cnt];
      g_max_query = new uint64_t[g_thread_cnt];
      memset(g_total_query, 0, sizeof(uint64_t) * g_thread_cnt);
      memset(g_fail_query, 0, sizeof(uint64_t) * g_thread_cnt);
      memset(g_max_query, 0, sizeof(uint64_t) * g_thread_cnt);
      int *id = new int[g_thread_cnt];

      ret = init_db();

      for (int i = 0; i < g_thread_cnt && 0 == ret; ++i) {
        id[i] = i;
        if (0 != (ret = pthread_create(&thread_id[i], NULL, task_runner, &id[i]))) {
          fprintf(stderr, "fail create new task. total=%d, i=%d, ret=%d\n", g_thread_cnt, i, ret);
          break;
        }
      }

      if (0 == ret) {
        pthread_t freeze_thread_id;
        if (0 != (ret = pthread_create(&freeze_thread_id, NULL, freeze_runner, NULL))) {
          fprintf(stderr, "fail create new util task. ret=%d\n", ret);
        }
      }

      for (int i = 0; i < g_thread_cnt && 0 == ret; ++i) {
        if (0 != (ret = pthread_join(thread_id[i], NULL))) {
          fprintf(stderr, "fail wait task finish. total=%d, i=%d, ret=%d\n", g_thread_cnt, i, ret);
          break;
        } else {
          // printf("thread %d join ok\n", i);
        }
      }
    }
    mysql_library_end();
  }

  printf("byebye\n");
  return ret;
}



