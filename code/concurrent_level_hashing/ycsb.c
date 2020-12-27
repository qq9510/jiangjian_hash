#include "level_hashing.h"


void test_insert(int thread_num){
    level_hash *level = level_init(22);
    level->thread_num = thread_num;
    uint64_t inserted = 0, queried = 0, t = 0;
    uint8_t key[KEY_LEN];
    uint8_t value[VALUE_LEN];

    FILE *ycsb, *ycsb_read;
    char *buf = NULL;
    size_t len = 0;
    struct timespec start, finish;
    double single_time;

    if ((ycsb = fopen("../data/load.text", "r")) == NULL)
    {
        perror("文件打开失败 load");
        return ;
    }

    printf("加载阶段开始 \n");
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (getline(&buf, &len, ycsb) != -1)
    {
        if (strncmp(buf, "INSERT", 6) == 0)
        {
            memcpy(key, buf + 7, KEY_LEN - 1);
            if (!level_insert(level, key, key))
            {
                inserted++;
            }
            else
            {
                break;
            }
        }
    }
    fclose(ycsb);
    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("数据加载完毕: %ld 项目已经插入。 用时 %f \n", inserted, single_time);


    clock_gettime(CLOCK_MONOTONIC, &start); 
    for (int i = inserted + 1; i < inserted + 1000000; i++)
    {
        snprintf(key, KEY_LEN, "%d", i);
        snprintf(value, VALUE_LEN, "%d", i);
        level_insert(level, key, value);          
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf(" 1000000 次插入数据结束。 用时 %f \n", single_time);

}

void test_wordload_a(int thread_num)
{

    level_hash *level = level_init(21);
    level->thread_num = thread_num;
    uint64_t inserted = 0, queried = 0, t = 0;
    uint8_t key[KEY_LEN];
    uint8_t value[VALUE_LEN];

    FILE *ycsb, *ycsb_read;
    char *buf = NULL;
    size_t len = 0;
    struct timespec start, finish;
    double single_time;

    if ((ycsb = fopen("../data/load.text", "r")) == NULL)
    {
        perror("文件打开失败 load");
        return ;
    }

    printf("加载阶段开始 \n");
    clock_gettime(CLOCK_MONOTONIC, &start);
    while (getline(&buf, &len, ycsb) != -1)
    {
        if (strncmp(buf, "INSERT", 6) == 0)
        {
            memcpy(key, buf + 7, KEY_LEN - 1);
            if (!level_insert(level, key, key))
            {
                inserted++;
            }
            else
            {
                break;
            }
        }
    }
    fclose(ycsb);
    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    printf("数据加载完毕: %ld 项目已经插入。 用时 %f \n", inserted, single_time);

    if ((ycsb_read = fopen("../data/wa.text", "r")) == NULL)
    {
        perror("文件打开失败 run");
        return ;
    }

    thread_queue *run_queue[thread_num];
    int move[thread_num];
    for (t = 0; t < thread_num; t++)
    {
        run_queue[t] = calloc(READ_WRITE_NUM / thread_num, sizeof(thread_queue));
        move[t] = 0;
    }

    int operation_num = 0;
    while (getline(&buf, &len, ycsb_read) != -1)
    {
        if (strncmp(buf, "INSERT", 6) == 0)
        {
            memcpy(run_queue[operation_num % thread_num][move[operation_num % thread_num]].key, buf + 7, KEY_LEN - 1);
            run_queue[operation_num % thread_num][move[operation_num % thread_num]].operation = 1;
            move[operation_num % thread_num]++;
        }
        else if (strncmp(buf, "READ", 4) == 0)
        {
            memcpy(run_queue[operation_num % thread_num][move[operation_num % thread_num]].key, buf + 5, KEY_LEN - 1);
            run_queue[operation_num % thread_num][move[operation_num % thread_num]].operation = 0;
            move[operation_num % thread_num]++;
        }
        else if (strncmp(buf, "UPDATE", 6) == 0)
        {
            memcpy(run_queue[operation_num % thread_num][move[operation_num % thread_num]].key, buf + 5, KEY_LEN - 1);
            run_queue[operation_num % thread_num][move[operation_num % thread_num]].operation = 2;
            move[operation_num % thread_num]++;
        }
        operation_num++;
    }
    fclose(ycsb_read);

    sub_thread *THREADS = (sub_thread *)malloc(sizeof(sub_thread) * thread_num);
    inserted = 0;

    printf("开始运行负载 \n");
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (t = 0; t < thread_num; t++)
    {
        THREADS[t].id = t;
        THREADS[t].level = level;
        THREADS[t].inserted = 0;
        THREADS[t].updated = 0;
        THREADS[t].hit = 0;
        THREADS[t].miss = 0;
        THREADS[t].run_queue = run_queue[t];
        pthread_create(&THREADS[t].thread, NULL, (void *)ycsb_thread_run, &THREADS[t]);
    }

 
    for (t = 0; t < thread_num; t++)
    {
        pthread_join(THREADS[t].thread, NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    int updated = 0;
    int hit = 0;
    int miss = 0;
    for (t = 0; t < thread_num; ++t)
    {
        updated += THREADS[t].updated;
        inserted += THREADS[t].inserted;
        hit += THREADS[t].hit;
        miss += THREADS[t].miss;
    }
    
    printf("负载运行结束: %ld/%ld/%d 项目被 搜索/插入/更新,用时%f秒 \n", operation_num - inserted- updated, inserted,updated,single_time);
    printf(" %d个查询命中，%d个查询没有命中\n ",hit,miss);
    printf("吞吐量: %f 个操作每秒 \n", READ_WRITE_NUM / single_time);
}

int main(int argc, char *argv[])
{

    int thread_num = atoi(argv[1]); // INPUT: the number of threads
    test_wordload_a(thread_num);

    return 0;
}
