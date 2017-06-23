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

uint32_t g_rand = 41;
int g_varchar_cols = 10;
const int g_varchar_width = 1024;
int g_thread_cnt = 0;
uint64_t *g_total_query = NULL;
uint64_t *g_fail_query = NULL;
int g_last_error_no = 0;
int g_thread_rows = 2000000;

void *freeze_runner(void *arg)
{
  int ret = 0;

  printf("freeze thread running\n");

  uint64_t last_total_query = 0;
  uint64_t last_fail_query = 0;

  int idle_times = 0;

  while (1) {
    uint64_t cur_total_query = 0;
    uint64_t cur_fail_query = 0;

    for (int i = 0; i < g_thread_cnt; ++i) {
      cur_fail_query += g_fail_query[i];
      cur_total_query += g_total_query[i];
    }

    if (cur_total_query - last_total_query > 10000) {
      fprintf(stderr, "PROGRESS:  succ/fail/total = %ld/%ld/%ld. about %lf GB written\n",
              cur_total_query - cur_fail_query, cur_fail_query, cur_total_query,
              1.0 * (cur_total_query - cur_fail_query) * g_varchar_cols * g_varchar_width / 1024 / 1024 /1024);
    }

    if (cur_fail_query - last_fail_query > 1000) {
      fprintf(stderr, "FAIL WARNING. succ/fail/total = %ld/%ld/%ld. last_errno=%d\n",
              cur_total_query - cur_fail_query, cur_fail_query, cur_total_query, g_last_error_no);
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

int build_create(char *create_sql, int buf_len, int cols, int thread_id)
{
  int pos = 0;
  int partition_count = 10;
  pos = sprintf(create_sql, "CREATE TABLE IF NOT EXISTS mybench_t%d("
          "primary key(pk1, pk2, pk3, pk4, pk5),"
          "pk1 int,"
          "pk2 int,"
          "pk3 int,"
          "pk4 int,"
          "pk5 int,", thread_id);
  for (int i = 1; i < cols; ++i) {
    pos += sprintf(create_sql + pos, "c%d varchar(1024),", i);
  }
  pos += sprintf(create_sql + pos, "c%d varchar(1024))", cols);
  pos += sprintf(create_sql + pos, " PARTITION BY KEY(pk1) PARTITIONS %d", partition_count);
  return pos;
}


int build_insert(char *buf, int buf_len, int cols, int thread_id)
{
  int scale = 100; // 用于选择 SCAN 数据范围
  int pos = 0;
  char randchar[g_varchar_width];

  pos = sprintf(buf, "INSERT INTO mybench_t%d VALUES(", thread_id);
  int i = 0;
  for (i = 0; i < 2; ++i) {
    int pk = (random() % scale) * exp(- 1.0 / (double)random());
    pos += sprintf(buf + pos, "%d,", pk);
  }
  for (; i < 5; ++i) {
    pos += sprintf(buf + pos, "%d,", random());
  }

  for (int i = 1; i < cols; ++i) {
    build_varchar(randchar, g_varchar_width);
    pos += sprintf(buf + pos, "'%s',", randchar);
  }

  build_varchar(randchar, g_varchar_width);
  pos += sprintf(buf + pos, "'%s')", randchar);
  return pos;
}

int make_load(MYSQL *conn, int thread_id)
{
  int ret = 0;
  // LOAD 特点：
  // 写入大批量数据，定时自动做 major freeze，统计执行失败次数，失败次数每上升 1
  // 个点打印一次执行统计，每执行 1000 个 query 打印一次执行统计。
  const int buf_len = g_varchar_cols * 1024 + 4096;
  char *sql_buf = new char[buf_len];
  int pos = 0;
  pos = build_create(sql_buf, buf_len, g_varchar_cols, thread_id);
  if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
    fprintf(stderr, "fail create table mybench_t%d. ret=%d\n", thread_id, ret);
  } else {
    printf("%s\n", sql_buf);
    for (int row = 0; row < g_thread_rows; ++row) {
      pos = build_insert(sql_buf, buf_len, g_varchar_cols, thread_id);
      // printf("%s\n", sql_buf);
      if (0 != (ret = mysql_real_query(conn, sql_buf, pos))) {
        g_fail_query[thread_id]++;
        g_last_error_no = mysql_errno(conn);
      }
      g_total_query[thread_id]++;
    }
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

  printf("task %d running\n", thread_id);

  if (0 == ret) {
    MYSQL *conn;
    mysql_thread_init();
    conn = mysql_init(NULL);
    if (conn == NULL) {
      printf("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    } else if (mysql_real_connect(conn, host, user, pass, db, atoi(port), NULL, 0) == NULL) {
      printf("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    } else if (0 != (ret = make_load(conn, thread_id))) {
      printf("Error %u: %s\n", mysql_errno(conn), mysql_error(conn));
    }
    if (NULL != conn) {
      mysql_close(conn);
    }
    mysql_thread_end();
  }

  pthread_exit(NULL);
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

  if (NULL == thread) {
    g_thread_cnt = 1; // default
    printf("note: provide THREAD_COUNT to enable multithread test\n");
  } else {
    g_thread_cnt = atoi(thread);
    printf("start %d thread\n", g_thread_cnt);
  }

  if (0 == ret) {
    if (0 != (ret = mysql_library_init(0, NULL, NULL))) {
      fprintf(stderr, "could not initialize MySQL library. ret=%d\n", ret);
    } else {
      pthread_t *thread_id = new pthread_t[g_thread_cnt];
      g_total_query = new uint64_t[g_thread_cnt];
      g_fail_query = new uint64_t[g_thread_cnt];
      memset(g_total_query, 0, sizeof(uint64_t) * g_thread_cnt);
      memset(g_fail_query, 0, sizeof(uint64_t) * g_thread_cnt);
      int *id = new int[g_thread_cnt];
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



