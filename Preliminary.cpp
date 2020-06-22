// 8  threads 0.2516
// 16 threads 0.2212
// 18 threads 0.2116

// without neon 0.2137

// #define TEST

#include<stdio.h>
#include<stdlib.h>
#include<sys/mman.h>    // linux mmap
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<unistd.h>
#include<stddef.h>
#include<limits.h>
#include<ctime>
#include<string.h>
#include<iostream>
#include<unordered_map>
#include<vector>
#include<algorithm>
#include <bits/stdc++.h>
#include<arm_neon.h>
using namespace std;

typedef unsigned int Uint;
const int MAX_EDGE_NUM = 280000;
const int MAX_NODE_NUM = 210000;
const int MAX_DEGREE_NUM = 51;

const int THREAD_COUNT = 18;


//18 threads
const int NUM_LEN3_RESULT = 500000;
const int NUM_LEN4_RESULT = 500000;
const int NUM_LEN5_RESULT = 1000000;
const int NUM_LEN6_RESULT = 2000000;
const int NUM_LEN7_RESULT = 3000000;
const int MAX_THREE = 10000;
int edge_num = 0;
int node_num = 0;
Uint U[MAX_EDGE_NUM];  //存储 id1
Uint V[MAX_EDGE_NUM];  //存储 id2
Uint ID[MAX_NODE_NUM];

Uint G_suc[MAX_NODE_NUM][MAX_DEGREE_NUM];
Uint G_pre[MAX_NODE_NUM][MAX_DEGREE_NUM]; 
int Out_Deg[MAX_NODE_NUM];
int In_Deg[MAX_NODE_NUM];

int valid_degree[MAX_NODE_NUM];
int total_valid_degree = 0;

//记录区间划分

int divide_region[THREAD_COUNT + 1];
int divide_region_index[THREAD_COUNT + 1];

// char C_ID[MAX_NODE_NUM * 10]; // *10 是因为Uint位数在1~10之间
// Uint C_ID_len[MAX_NODE_NUM]; //记录每个 id 的长度，用于读取在C_ID 存放的 char 数据

//因为测试 id 不超过 7 位, + ',' or '\n' 
char C_Comma[MAX_NODE_NUM * 8];
char C_LF[MAX_NODE_NUM * 8];
int C_len[MAX_NODE_NUM];



//逆 3 表
struct Three
{
    Uint u;
    Uint v;
    Uint k;
    Three() : u(), v(), k() {}
    Three(Uint u, Uint v, Uint k) : u(u), v(v), k(k) {} 
};


struct Result
{
    int per_len_num[5] = {0};
    Uint res3[NUM_LEN3_RESULT][3];
    Uint res4[NUM_LEN4_RESULT][4];
    Uint res5[NUM_LEN5_RESULT][5];
    Uint res6[NUM_LEN6_RESULT][6];
    Uint res7[NUM_LEN7_RESULT][7];
    Uint path[7];
    int left;
    int right;
    int circle_num = 0;
    Uint currentJs[500];
    Uint num_currentJS;
    bool visited[MAX_NODE_NUM];
    int reachable[MAX_NODE_NUM];
    Three three[MAX_THREE];
    Uint three_len = 0;
};

const int FILE_LENGTH = 134217728; // 128 * 1024 * 1024 
char res_str[FILE_LENGTH];

Result infos[THREAD_COUNT];

Uint countSortID[MAX_NODE_NUM]; // 索引为值，元素为 1 表示 该 id 存在。

//                       函数原型声明
//==========================================================
void analyse(char *buf, const long len);
void read_data(char *filename);

//https://zhuanlan.zhihu.com/p/33638344
inline int digits10_v3(Uint v);
int u32ToAscii_v2(Uint value, char *dst);

inline int digits10_v4(Uint v);
int u32ToAscii_v3(Uint value, char *Comma, char *LF);
void dfs(Uint start, Uint cur_id, int depth, Result* info);
void pre_dfs(Uint start, Uint cur_id, int depth, Result* info);
void *sub_dfs(void *arg);
void *memcpy_16(void *dest, void *src);
void write_result(char *resultFile);


// #include<arm_neon.h>
void *memcpy_16(void *dest, void *src)
{
    unsigned long *s = (unsigned long *)src;
    unsigned long *d = (unsigned long *)dest;
    vst1q_u64(&d[0], vld1q_u64(&s[0]));
    return dest;
}


void analyse(char *buf, const long len)
{   
    register int ids = -1;
    register int j = 0;
    register Uint temp = 0;

    for (char *p = buf; *p && p - buf < len; ++p)
    {
        if(*p != ',')
        {
            // atoi
            temp = (temp << 1) + (temp << 3) + *p - '0';
            // temp = temp * 10 + *p - '0'; 
        }
        else //if(*p == ',')  这里不写成 if(*p == ',') 是为了减少一次对 *p == ',' 的判断
        {
            ++j;
            countSortID[temp] = 1;

            // if(temp > MAX_ID) //记录最大的 id
            // {
            //     MAX_ID = temp;
            // }
            switch (j)
            {
                case 1:   //读取 id1
                    U[edge_num] = temp;
                
                    // ID[++ids] = temp;  不使用 ID + sort + 删除重复来给 id 排序                    
                    
                    temp = 0;
                    break;
                case 2:   //读取 id2
                    
                    V[edge_num++] = temp;
                               
                    // ID[++ids] = temp;
                
                    temp = 0;

                    while(*p != '\n') //在这里和 for里面的 ++p 将 '\n' 过滤掉
                        ++p;
                    j = 0;  // 开始读取下一行
                    break;     
            }
        }      
    }
}

void read_data(char *filename)
{
#ifdef TEST
    clock_t start = clock();
#endif
    int fd = open(filename, O_RDONLY);
    long length = lseek(fd, 0, SEEK_END);
    char *mbuf = (char *)mmap(NULL, length, PROT_READ, MAP_PRIVATE, fd, 0);
    analyse(mbuf, length);
    close(fd);
    munmap(mbuf, length);

#ifdef TEST
    clock_t end = clock();
    printf("read data time:%0.5fs\n", double(end - start)/CLOCKS_PER_SEC);
#endif
}

inline int digits10_v3(Uint v)
{
    if(v < 10) return 1;
    if(v < 100) return 2;
    if(v < 1000) return 3;
    if(v < 1e6)
    {
        if(v < 1e4) return 4;
        return 5 + (v >= 1e5);
    }
    if(v < 1e8)
    {
        if(v < 1e7)return 7;
        return 7+(v >= 1e7);
    }
    if(v < 1e10)
    {
        if(v < 1e9)return 9;
        return 9 + (v >= 1e9);
    }
    return 10;
}


//通过测试 知 id不会超过 6 位
inline int digits10_v4(Uint v)
{
    if(v < 1e4)
    {
        if(v < 1000)
        {
            if(v < 10)return 1;
            
            return (v > 100) ? 3 : 2; 
        }
        return 4;
    }
    else
    {
        return (v < 1e5) ? 5 : 6;
    }
}

int u32ToAscii_v2(Uint value, char *dst)
{
    static const char digits[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

    const Uint length = digits10_v3(value);

    Uint next = length - 1;

    while (value >= 100)
    {
        const Uint i = (value % 100) << 1;
        value /= 100;
        dst[next - 1] = digits[i];
        dst[next] = digits[i + 1];
        next -= 2;
    }
    if(value < 10)
    {
        dst[next] = '0' + value;
    }
    else
    {
        Uint i = value << 1;
        dst[next - 1] = digits[i];
        dst[next] = digits[i + 1];
    }
    return length;
}



int u32ToAscii_v3(Uint value, char *Comma, char *LF)
{
    static const char digits[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

    const int length = digits10_v4(value);

    int next = length - 1;

    while (value >= 100)
    {
        const int i = (value % 100) << 1;
        value /= 100;
        Comma[next - 1] = digits[i];
        Comma[next] = digits[i + 1];
        LF[next - 1] = digits[i];
        LF[next] = digits[i + 1];
        next -= 2;
    }
    if(value < 10)
    {
        Comma[next] = '0' + value;
        LF[next] = '0' + value;
    }
    else
    {
        int i = value << 1;
        Comma[next - 1] = digits[i];
        Comma[next] = digits[i + 1];
        
        LF[next - 1] = digits[i];
        LF[next] = digits[i + 1];
    }
    Comma[length] = ',';
    LF[length] = '\n';
    return length + 1;
}

//=============================================

void pre_dfs(Uint start_id, Uint cur_id, int depth, Result* info)
{
    Uint begin = 0;
    if (depth < 2)
    {
        while (start_id >= G_pre[cur_id][begin])
            ++begin;
    }
    else
    {
        while (start_id > G_pre[cur_id][begin])
            ++begin;
    }

    for (; begin < In_Deg[cur_id]; ++begin)
    {
        unsigned int next_id = G_pre[cur_id][begin];
        if (!info->visited[next_id])
        {
            info->path[depth] = next_id;

            if (depth == 2)
            {
                if (info->path[2] != info->path[0])
                    info->three[info->three_len++] = {info->path[2], info->path[1], info->path[0]};
            }
            else
            {
                info->visited[next_id] = true;
                pre_dfs(start_id, next_id, depth + 1, info);
                info->visited[next_id] = false;
            }
        }
    }
}


void dfs(Uint start_id, Uint cur_id, int depth, Result* info)
{
    int count;
    if (info->reachable[cur_id])
    {        
        switch (depth)
        {   
            case 0:

                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        count = info->per_len_num[depth]++;
                        memcpy(info->res3[count], info->path, depth << 2);
                        // info->res3[count][0] = cur_id;
                        // info->res3[count][1] = info->three[i].v;
                        // info->res3[count][2] = info->three[i].k;
                        memcpy(info->res3[count], &info->three[i].u, 12);
                    }
                }
            break;

            case 1:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        count = info->per_len_num[depth]++;
                        memcpy(info->res4[count], info->path, depth << 2);
                        // info->res4[count][1] = cur_id;
                        // info->res4[count][2] = info->three[i].v;
                        // info->res4[count][3] = info->three[i].k;
                        memcpy(info->res4[count] + 1, &info->three[i].u, 12);
                    }
                }
                break;

            case 2:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        count = info->per_len_num[depth]++;
                        memcpy(info->res5[count], info->path, depth << 2);
                        // info->res5[count][2] = cur_id;
                        // info->res5[count][3] = info->three[i].v;
                        // info->res5[count][4] = info->three[i].k;
                        memcpy(info->res5[count] + 2, &info->three[i].u, 12);
                    }
                }
                break;

            case 3:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        count = info->per_len_num[depth]++;
                        memcpy(info->res6[count], info->path, depth << 2);
                        // info->res6[count][3] = cur_id;
                        // info->res6[count][4] = info->three[i].v;
                        // info->res6[count][5] = info->three[i].k;
                        memcpy(info->res6[count] + 3, &info->three[i].u, 12);
                    }
                }
                break;

            case 4:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        count = info->per_len_num[depth]++;
                        memcpy(info->res7[count], info->path, depth << 2);
                        // info->res7[count][4] = cur_id;
                        // info->res7[count][5] = info->three[i].v;
                        // info->res7[count][6] = info->three[i].k;
                        memcpy(info->res7[count] + 4, &info->three[i].u, 12);
                    }
                }
                break;

            default:
                break;
        }
    }

    if (depth < 4)
    {
        Uint begin = 0;
        while (start_id >= G_suc[cur_id][begin])
            ++begin;

        info->visited[cur_id] = true;
        info->path[depth] = cur_id;

        for (; begin < Out_Deg[cur_id]; ++begin)
        {
            if (!info->visited[G_suc[cur_id][begin]])
            {
                dfs(start_id, G_suc[cur_id][begin], depth + 1, info);
            }
        }
        info->visited[cur_id] = false;
    }
}
//=============================================


inline bool cmp(Three &a, Three &b)
{
    // if (a.u != b.u)
    //     return a.u < b.u;
    // else if (a.v != b.v)
    //     return a.v < b.v;
    // else
    //     return a.k < b.k;

    return (a.u != b.u) ? a.u < b.u : (a.v != b.v ? a.v < b.v : a.k < b.k);
}

void *sub_dfs(void *arg)
{
    clock_t start_time = clock();
    
    Result* info = (Result*)arg;

    for(Uint i = info->left; i < info->right; ++i)
    {
 
        pre_dfs(i, i, 0, info);

        if(info->three_len)
	    {
            sort(info->three, info->three + info->three_len, cmp);

            info->three[info->three_len] = {INT_MAX, INT_MAX, INT_MAX};

            for(Uint i = 0; i < info->three_len; ++i)
            {
                if(!info->reachable[info->three[i].u])
                {
                    info->reachable[info->three[i].u] = i + 1;
                    info->currentJs[(info->num_currentJS)++] = info->three[i].u;
                }
            }
   
        }
        dfs(i, i, 0, info);
        for(Uint k = 0; k < info->num_currentJS; ++k)
        {
            info->reachable[info->currentJs[k]] = 0;
        }
        info->num_currentJS = 0;
        info->three_len = 0;
    }
#ifdef TEST
    clock_t end_time = clock();
    printf("circle num %d ;dfs time :%0.5fs\n",info->circle_num, double(end_time - start_time)/CLOCKS_PER_SEC);
#endif
    
    pthread_exit(NULL);
}

void write_result(char *resultFile)
{
#ifdef TEST
    clock_t start = clock();
#endif
    FILE *fp = fopen(resultFile, "w");
    // char *res_str = (char *)malloc(256 * 1024 * 1024);

    int count_loop = 0;

    for(int i = 0; i < THREAD_COUNT; ++i)
    {
        for(int j = 0; j < 5; ++j)
        {
            count_loop += infos[i].per_len_num[j];
        }
	    
    }
#ifdef TEST
    printf("total circle num : %d\n", count_loop);
#endif
    register int res_len = u32ToAscii_v2(count_loop, res_str);;
    res_str[res_len++] = '\n';

    //len 3
    register int k;
    register int i;
    register int j;
    register int index;
    for(k = 0; k < THREAD_COUNT; k++)
    {
        //__builtin_prefetch(infos[k].res3 + infos[k].per_len_num[0] * 3, 0);
        for(i = 0; i < infos[k].per_len_num[0]; ++i)
        {
            
            for(j = 0; j < 2; ++j)
            {
                index = infos[k].res3[i][j];
                memcpy_16(res_str + res_len, C_Comma + (index << 3));
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res3[i][2];
            memcpy_16(res_str + res_len, C_LF + (index << 3));
            res_len += C_len[index];
            // res_str[res_len++] = '\n';
        }
    }
    //len 4
    for(k = 0; k < THREAD_COUNT; k++)
    {
        //__builtin_prefetch(infos[k].res4 + infos[k].per_len_num[1] * 4, 0);
        for(i = 0; i < infos[k].per_len_num[1]; ++i)
        {
            for(j = 0; j < 3; ++j)
            {
                index = infos[k].res4[i][j];
                memcpy_16(res_str + res_len, C_Comma + (index << 3));
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res4[i][3];
            memcpy_16(res_str + res_len, C_LF + (index << 3));
            res_len += C_len[index];
            // res_str[res_len++] = '\n';

        }
    }
    //len 5
    for(k = 0; k < THREAD_COUNT; k++)
    {
        //__builtin_prefetch(infos[k].res5 + infos[k].per_len_num[2] * 5, 0);
        for(i = 0; i < infos[k].per_len_num[2]; ++i)
        {
            for(j = 0; j < 4; ++j)
            {
                index = infos[k].res5[i][j];
                memcpy_16(res_str + res_len, C_Comma + (index << 3));
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res5[i][4];
            memcpy_16(res_str + res_len, C_LF + (index << 3));
            res_len += C_len[index];
            // res_str[res_len++] = '\n';

        }
    }
    // len 6
    for(k = 0; k < THREAD_COUNT; k++)
    {
        //__builtin_prefetch(infos[k].res6 + infos[k].per_len_num[3] * 6, 0);
        for(i = 0; i < infos[k].per_len_num[3]; ++i)
        {
            for(j = 0; j < 5; ++j)
            {
                index = infos[k].res6[i][j];
                memcpy_16(res_str + res_len, C_Comma + (index << 3));
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res6[i][5];
            memcpy_16(res_str + res_len, C_LF + (index << 3));
            res_len += C_len[index];
            // res_str[res_len++] = '\n';
        }
    }
    //len 7
    for(k = 0; k < THREAD_COUNT; k++)
    {
        //__builtin_prefetch(infos[k].res7 + infos[k].per_len_num[4] * 7, 0);
        for(i = 0; i < infos[k].per_len_num[4]; ++i)
        {
            for(j = 0; j < 6; ++j)
            {
                index = infos[k].res7[i][j];
                memcpy_16(res_str + res_len, C_Comma + (index << 3));
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res7[i][6];
            memcpy_16(res_str + res_len, C_LF + (index << 3));
            res_len += C_len[index];
            // res_str[res_len++] = '\n';
        }
    }
    res_str[res_len] = '\0';
    fwrite(res_str, sizeof(char), res_len, fp);
    fclose(fp);
#ifdef TEST
    clock_t end_time = clock();
    printf("write result time:%0.5fs\n", double(end_time - start)/CLOCKS_PER_SEC);
#endif
}







int main()
{


#ifdef TEST
    clock_t END, START = clock();
    char testFile[] = "3738/test_data.txt";
    char resultFile[] ="3738/a.txt";
#else
    char testFile[] = "/data/test_data.txt";
    char resultFile[] = "/projects/student/result.txt";
#endif
    
    read_data(testFile);

    //通过计数排序来做节点映射， 比 algorithm 里的 sort 效果好太多。。。
    for(Uint id = 1; id < MAX_NODE_NUM; ++id)
    {
       countSortID[id]  += countSortID[id - 1];
    }
    node_num = countSortID[MAX_NODE_NUM - 1];


    register Uint u;
    register Uint v;
    register int i;
    for (i = 0; i < edge_num; i += 4)
    {
        u = countSortID[U[i]] - 1; 
        v = countSortID[V[i]] - 1;
        ID[u] = U[i];
        ID[v] = V[i];
        G_suc[u][Out_Deg[u]++] = v;
        G_pre[v][In_Deg[v]++] = u;

        u = countSortID[U[i + 1]] - 1; 
        v = countSortID[V[i + 1]] - 1;
        ID[u] = U[i + 1];
        ID[v] = V[i + 1];
        G_suc[u][Out_Deg[u]++] = v;
        G_pre[v][In_Deg[v]++] = u;

        u = countSortID[U[i + 2]] - 1; 
        v = countSortID[V[i + 2]] - 1;
        ID[u] = U[i + 2];
        ID[v] = V[i + 2];
        G_suc[u][Out_Deg[u]++] = v;
        G_pre[v][In_Deg[v]++] = u;

        u = countSortID[U[i + 3]] - 1; 
        v = countSortID[V[i + 3]] - 1;
        ID[u] = U[i + 3];
        ID[v] = V[i + 3];
        G_suc[u][Out_Deg[u]++] = v;
        G_pre[v][In_Deg[v]++] = u;

        //用于线程顶点划分策略
        // if(v > u)
        // {
        //     ++total_valid_degree;
        //     ++valid_degree[u];
        // }
    }
    for(; i < edge_num; ++i)
    {
        u = countSortID[U[i]] - 1; 
        v = countSortID[V[i]] - 1;
        ID[u] = U[i];
        ID[v] = V[i];
        G_suc[u][Out_Deg[u]++] = v;
        G_pre[v][In_Deg[v]++] = u;
    }

    for (i = 0; i < node_num;  i += 4)
    {
        sort(G_suc[i], G_suc[i] + Out_Deg[i]);
        sort(G_pre[i], G_pre[i] + In_Deg[i]);
        G_suc[i][Out_Deg[i]] = INT_MAX;
        G_pre[i][In_Deg[i]] = INT_MAX;
        C_len[i] = u32ToAscii_v3(ID[i], C_Comma  + (i << 3), C_LF + (i << 3));

        sort(G_suc[i + 1], G_suc[i + 1] + Out_Deg[i + 1]);
        sort(G_pre[i + 1], G_pre[i + 1] + In_Deg[i + 1]);
        G_suc[i + 1][Out_Deg[i + 1]] = INT_MAX;
        G_pre[i + 1][In_Deg[i + 1]] = INT_MAX;
        C_len[i + 1] = u32ToAscii_v3(ID[i + 1], C_Comma  + ((i + 1)<< 3), C_LF + ((i + 1) << 3));
    
        sort(G_suc[i + 2], G_suc[i + 2] + Out_Deg[i + 2]);
        sort(G_pre[i + 2], G_pre[i + 2] + In_Deg[i + 2]);
        G_suc[i + 2][Out_Deg[i + 2]] = INT_MAX;
        G_pre[i + 2][In_Deg[i + 2]] = INT_MAX;
        C_len[i + 2] = u32ToAscii_v3(ID[i + 2], C_Comma  + ((i + 2)<< 3), C_LF + ((i + 2) << 3));

        sort(G_suc[i + 3], G_suc[i + 3] + Out_Deg[i + 3]);
        sort(G_pre[i + 3], G_pre[i + 3] + In_Deg[i + 3]);
        G_suc[i + 3][Out_Deg[i + 3]] = INT_MAX;
        G_pre[i + 3][In_Deg[i + 3]] = INT_MAX;
        C_len[i + 3] = u32ToAscii_v3(ID[i + 3], C_Comma  + ((i + 3)<< 3), C_LF + ((i + 3) << 3));
    }
    
    for(; i < node_num; ++i)
    {
        sort(G_suc[i], G_suc[i] + Out_Deg[i]);
        sort(G_pre[i], G_pre[i] + In_Deg[i]);
        G_suc[i][Out_Deg[i]] = INT_MAX;
        G_pre[i][In_Deg[i]] = INT_MAX;
        C_len[i] = u32ToAscii_v3(ID[i], C_Comma  + (i << 3), C_LF + (i << 3));
    }

    // 用于线程顶点划分策略
    // for(int i = 1; i <= THREAD_COUNT; i++)
    // {
    //     divide_region[i] = int(total_valid_degree * i / THREAD_COUNT);
    // }

    // int acc_degree = 0;
    // for(int i = 0; i < node_num; ++i)
    // {
    //     acc_degree += valid_degree[i];
        
    //     if(acc_degree < divide_region[1])
    //     {
    //         divide_region_index[1] = i;
    //         continue;
    //     }
    //     else if(acc_degree < divide_region[2])
    //     {
    //         divide_region_index[2] = i;
    //         continue;
    //     }
    //     else if(acc_degree < divide_region[3])
    //     {
    //         divide_region_index[3] = i;
    //         continue;
    //     }
    //     else
    //     {
    //         divide_region_index[4] = node_num;
    //         break;
    //     }
    // }

    
#ifdef TEST
    clock_t start_time = clock();
#endif

#ifdef TEST
    clock_t end_time = clock();
    printf("construct U_V_K time :%0.5fs\n", double(end_time - start_time)/CLOCKS_PER_SEC);
#endif

//  参考 ddd 的多线程代码
    pthread_t threads[THREAD_COUNT];
    pthread_attr_t attr;
    void *status;


    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   
    // 顶点数量均分策略
    int w = node_num / THREAD_COUNT;

    for(int i = 0; i < THREAD_COUNT; i++)
    {
	    infos[i].left = w * i;
	    infos[i].right = w * (i + 1);
    }
    infos[THREAD_COUNT - 1].right = node_num;

    //按照有效总出度占比划分策略
    // for(int i = 0; i < THREAD_COUNT; i++)
    // {
    //     infos[i].left = divide_region_index[i];
    //     infos[i].right = divide_region_index[i + 1];
    // }

    for(int i = 0; i < THREAD_COUNT; i++)
    {
        int rc = pthread_create(&threads[i], NULL, sub_dfs, (void*)&(infos[i]));
        if(rc)
        {
            cout<<"pthread create error\n";
            exit(-1);
        }
    }

    pthread_attr_destroy(&attr);
    for(int i = 0; i < THREAD_COUNT; ++i)
    {
        int rc = pthread_join(threads[i], &status);
        if(rc)
        {
            cout<<"pthread join error\n";
            exit(-1);
        }
    }
    
     write_result(resultFile);


#ifdef TEST
    END = clock();

    printf("total time:%0.5fs\n", double(END - START)/CLOCKS_PER_SEC);
#endif
    //pthread_exit(NULL);
    return 0;
}
