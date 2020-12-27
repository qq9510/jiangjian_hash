#include "level_hashing.h"

void test(){

    int level_size = 20; 
    int insert_num = 20000000;
   
    level_hash *level = level_init(level_size);
    uint64_t inserted = 0, i = 0;
    uint8_t key[KEY_LEN];
    uint8_t value[VALUE_LEN];

    struct timespec start, finish;
    double single_time;

    level->total_move = 0;

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (i = 1; i < insert_num + 1; i++)
    {
        snprintf(key, KEY_LEN, "%ld", i);
        snprintf(value, VALUE_LEN, "%ld", i);
        if (!level_insert(level, key, value))
        {
            inserted++; 
        }
        else
        {
            printf("扩容: 空间利用率 & 总entries数: %f  %ld\n",
                   (float)(level->level_item_num[0] + level->level_item_num[1]) / (level->total_capacity * ASSOC_NUM),
                   level->total_capacity * ASSOC_NUM);
            level_expand(level);
            level_insert(level, key, value);
            inserted++;
        }
        struct timespec s, e;
        double t;
        if(inserted% 250000 == 0 && inserted!=insert_num){ 
                clock_gettime(CLOCK_MONOTONIC, &s);
                level->move_time = 0;
                for (int k = 0; k < 100000; k++){
                    i++;
                    snprintf(key, KEY_LEN, "%ld", i);
                    snprintf(value, VALUE_LEN, "%ld", i);

                    if (!level_insert(level, key, value))
                    {
                        inserted++; 
                    }
                    else
                    {
                        printf("扩容: 空间利用率 & 总entries数: %f  %ld\n",
                            100* (float)(level->level_item_num[0] + level->level_item_num[1]) / (level->total_capacity * ASSOC_NUM),
                            level->total_capacity * ASSOC_NUM);
                        level_expand(level);
                        level_insert(level, key, value);
                        inserted++;
                    }         
                }

                clock_gettime(CLOCK_MONOTONIC, &e);
                t =  (e.tv_sec - s.tv_sec) + (e.tv_nsec - s.tv_nsec) / 1000000000.0;
            //  printf("%ld, %ld \n",e.tv_sec, s.tv_sec);
            //  printf("%ld, %ld \n",e.tv_nsec/1000, s.tv_nsec/1000);
                
                float  space_use =100*  (float)(level->level_item_num[0] + level->level_item_num[1]) / (level->total_capacity * ASSOC_NUM);
                printf("空间利用率：%f,记录总数为：%ld ,100000次插入时间为: %f毫秒,尝试移动次数:%d \n", 
                space_use, inserted, t *1000 ,level->move_time);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &finish);
    single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf(" %ld 项目已经插入。 用时 %f \n", inserted, single_time);

    printf("发生 %d 次数据移动调用。 占比 %f \n", level->total_move,  1.0* level->total_move/inserted);

    // clock_gettime(CLOCK_MONOTONIC, &start); 
    // for (i = 1; i < insert_num + 1; i++)
    // {
    //     snprintf(key, KEY_LEN, "%ld", i);
    //     uint8_t *get_value = level_dynamic_query(level, key);
    //     if (get_value == NULL)
    //         printf("Search the key %s: ERROR! \n", key);
    // }
    // clock_gettime(CLOCK_MONOTONIC, &finish);
    // single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    // printf(" %d 次动态查询结束。 用时 %f \n", insert_num, single_time);


    // clock_gettime(CLOCK_MONOTONIC, &start); 
    // for (i = insert_num + 1; i < insert_num *2; i++)
    // {
    //     snprintf(key, KEY_LEN, "%ld", i);
    //     uint8_t *get_value = level_dynamic_query(level, key);
    // }
    // clock_gettime(CLOCK_MONOTONIC, &finish);
    // single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    // printf(" %d 次动态不存在的数据查询结束。 用时 %f \n", insert_num, single_time);


    // for (i = 1; i < insert_num + 1; i++)
    // {
    //     snprintf(key, KEY_LEN, "%ld", i);
    //     snprintf(value, VALUE_LEN, "%ld", i * 2);
    //     if (level_update(level, key, value))
    //         printf("Update the value of the key %s: ERROR! \n", key);
    // }

    // for (i = 1; i < insert_num + 1; i++)
    // {
    //     snprintf(key, KEY_LEN, "%ld", i);
    //     if (level_delete(level, key))
    //         printf("Delete the key %s: ERROR! \n", key);
    // }

    level_destroy(level);
}

int main(int argc, char *argv[])
{
   // int level_size = atoi(argv[1]); 
   // int insert_num = atoi(argv[2]); 

    test();
    return 0;
}
