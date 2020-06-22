/*初见前路近可至,细思百事竞待忙*/

// #define TEST
#define LINUX_MMAP
#define NEON
#define LF_RN

#include <stdio.h>
#include<limits.h>
#include <bits/stdc++.h>

#ifdef LINUX_MMAP
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#ifdef NEON
#include<arm_neon.h>
#endif

#include <ext/pb_ds/hash_policy.hpp>
#include <ext/pb_ds/assoc_container.hpp>

using namespace std;

typedef unsigned int Uint;

// 边数不超过 200w 顶点数不超过 400w 
const int MAX_EDGE_NUM = 2000000;
const int MAX_NODE_NUM = 2100000;

// 定义每个线程 不同长度环的最大数量
// const int LEN_3_NUM = 20000000;
// const int LEN_4_NUM = 20000000;
// const int LEN_5_NUM = 20000000;
// const int LEN_6_NUM = 20000000;
// const int LEN_7_NUM = 20000000;

const int LEN_3_NUM = 5000000;
const int LEN_4_NUM = 4000000;
const int LEN_5_NUM = 4000000;
const int LEN_6_NUM = 4500000;
const int LEN_7_NUM = 3600000;

// 找环的线程数
const int THREAD_NUM = 18;

float pthread_ratio[19] = {0, 0.011, 0.022, 0.033, 0.0444, 0.055, 0.066, 0.077, 0.1, 0.12, 0.13, 0.15, 0.18, 0.22, 0.25, 0.29, 0.523, 0.75, 1};

const int MAX_ANTI_3_NODE_NUM = 10000000;

// const int OUTPUT_LENGTH = 1073741824; // 1024^3 开 1G 的空间

const int OUTPUT_LENGTH = 1540000000; // 2000w * 7 * 11 
//记录真实的边数 定点数

char res_str[OUTPUT_LENGTH];
int offset[5] = {0};

int edge_num = 0, node_num = 0;

struct Edge
{
    Uint id1, id2, money;

    Edge() {}

    Edge(Uint id1, Uint id2, Uint money) : id1(id1), id2(id2), money(money){}
};
__gnu_pbds::gp_hash_table<Uint, int> id_hash;
//unordered_map<Uint, int> id_hash;
Edge graph_suc[MAX_EDGE_NUM];
Edge graph_pre[MAX_EDGE_NUM];

// int suc_index[MAX_NODE_NUM];
// int pre_index[MAX_NODE_NUM];

//========================================
int suc_index[MAX_NODE_NUM][2];
int pre_index[MAX_NODE_NUM][2];
//========================================
Uint ID[MAX_NODE_NUM * 2];

//因为 id 长度最长为 10 位, + ',' or '\n' 10 + 1 => 11 
char Comma[MAX_NODE_NUM * 11];
char LF[MAX_NODE_NUM * 11];
char C_len[MAX_NODE_NUM];


//逆 3 表
//添加了 money 数据
struct Three
{
    Uint u;
    Uint v;
    Uint k;
    Uint X;  // X，Y 对应于 文档中的 Y / X
    Uint Y;
    Three() : u(), v(), k(), X(), Y() {}
    Three(Uint u, Uint v, Uint k, Uint X, Uint Y) : u(u), v(v), k(k), X(X), Y(Y) {} 
};

struct Result
{
    int per_len_num[5] = {0};
    int char_len_num[5] = {0};
    Uint res3[LEN_3_NUM][3];
    Uint res4[LEN_4_NUM][4];
    Uint res5[LEN_5_NUM][5];
    Uint res6[LEN_6_NUM][6];
    Uint res7[LEN_7_NUM][7];
    Uint path[4];
    Uint money[4];
    int left; 
    int right;
    int circle_num = 0;
    Uint currentJs[MAX_ANTI_3_NODE_NUM];
    Uint num_currentJS;
    bool visited[MAX_NODE_NUM];
    int reachable[MAX_NODE_NUM];
    Three three[MAX_ANTI_3_NODE_NUM];
    Uint three_len = 0;
};



Result infos[THREAD_NUM];

#ifdef NEON
void *memcpy_16(void *dest, void *src)
{
    unsigned long *s = (unsigned long *)src;
    unsigned long *d = (unsigned long *)dest;
    vst1q_u64(&d[0], vld1q_u64(&s[0]));
    return dest;
}
#endif

#ifdef LINUX_MMAP

// 官方数据换行符为 \r\n
void analyse(char *buf, const long len)
{   
    register int ids = -1;
    int j = 0;
    register Uint temp = 0;

    for (char *p = buf; *p && p - buf < len; ++p)
    {

#ifdef LF_RN
        if(*p != ',' && *p != '\r')
#else
        if(*p != ',' && *p != '\n')
#endif
        {
            temp = (temp << 1) + (temp << 3) + *p - '0';
	    }   
        else
        {
            ++j;
            switch (j)
            {
                case 1:  //读取 id1
                    graph_suc[edge_num].id1 = temp;
                    graph_pre[edge_num].id1 = temp;
                    // countSortID[temp] = 1;
                    ID[++ids] = temp;
                    temp = 0;
                    break;
                case 2:   //读取 id2
                    graph_suc[edge_num].id2 = temp;
                    graph_pre[edge_num].id2 = temp;
                    // countSortID[temp] = 1;
                    ID[++ids] = temp;
                    temp = 0;
                    break;
                case 3:
                    graph_suc[edge_num].money = temp;
                    graph_pre[edge_num].money = temp;
		            ++edge_num;
                    temp = 0;
                    j = 0;
#ifdef LF_RN
                    ++p;
#endif
                    break;  
            }
        }
    }
}

void read_data_mmap(char *filename)
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
    printf("mmap read data time:%0.5fs\n", double(end - start)/CLOCKS_PER_SEC);
#endif
}
#endif

void read_data_fscanf(char * filename)
{
#ifdef TEST
    clock_t start = clock();
#endif
    int ids = -1;
    FILE *file = fopen(filename, "r");
    // unsigned int money;
    while (fscanf(file, "%d,%d,%d\n", graph_suc[edge_num].id1, graph_suc[edge_num].id2, graph_suc[edge_num].money) != EOF)
    {
        graph_pre[edge_num] = {graph_suc[edge_num].id1, graph_suc[edge_num].id2, graph_suc[edge_num].money};
        ID[++ids] = graph_suc[edge_num].id1;
        ID[++ids] = graph_suc[edge_num].id2;
        ++edge_num;
    }
    fclose(file);
#ifdef TEST
    printf("win10 read data time : %fs\n ",(double)(clock() - start) / CLOCKS_PER_SEC);
#endif
}

bool suc_cmp(Edge &a, Edge &b)
{
    return a.id1 < b.id1 || (a.id1 == b.id1 && a.id2 < b.id2);
}

bool pre_cmp(Edge &a, Edge &b)
{
    return a.id2 < b.id2 || (a.id2 == b.id2 && a.id1 < b.id1);
}

void removeDuplicates() 
{
    register int i = 0;
    register int n = edge_num << 1;
    for (int j = 1; j < n; j++) {
        if (ID[j] != ID[i]) {
            i++;
            ID[i] = ID[j];
        }
    }
    node_num =  i + 1;
}

// digits10 
inline int digits10(Uint v)
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

int u32ToAscii(Uint value, char *Comma, char *LF)
{
    static const char digits[] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

    const int length = digits10(value);

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

inline bool judge(Uint &X, Uint &Y)
{
    if(X > Y)
    {
       if(Y & 0x40000000)
           return true;
       else
           return X - Y <= (Y << 2);
    
    }
    else
    {
       return Y - X <= (X << 1);
    }
    // return X > Y ? ((Y & 0x40000000) ? true : X - Y <= (Y << 2)) : Y - X <= (X << 1);
}

void pre_dfs(int start_id, int cur_id, int depth, Result* info)
{

    if(!pre_index[cur_id][0])
        return;

    int begin = pre_index[cur_id][0] - 1, max_index = begin + pre_index[cur_id][1];

    if(depth < 2)
    {
        while(begin <= max_index && start_id >= graph_pre[begin].id1)
            ++begin;
    }
    else
    {
        while(begin <= max_index && start_id > graph_pre[begin].id1)
            ++begin;
    }

    for(; begin <= max_index; ++begin)
    {
        int next_id = graph_pre[begin].id1;
        Uint next_id_money = graph_pre[begin].money;

        if(!info->visited[next_id] && (!depth || judge(next_id_money, info->money[depth - 1])))
        {
            info->path[depth] = next_id;
            info->money[depth] = next_id_money;

            if(depth == 2)
            {
                if(info->path[2] != info->path[0])
                {
                    if(judge(info->money[2], info->money[1]) && judge(info->money[1], info->money[0]))
                        info->three[info->three_len++] = {info->path[2], info->path[1], info->path[0], info->money[2], info->money[0]};
                }
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
                        if(judge(info->three[i].Y, info->three[i].X))
                        {
                            count = info->per_len_num[depth]++;
#ifdef NEON
                            memcpy_16(info->res3[count], info->path);
                            memcpy_16(info->res3[count], &info->three[i].u);

                            info->char_len_num[0] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];

#else
                            memcpy(info->res3[count], info->path, depth << 2);
                            info->res3[count][0] = cur_id;
                            info->res3[count][1] = info->three[i].v;
                            info->res3[count][2] = info->three[i].k;
                            info->char_len_num[0] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
#endif
                        }
                    }
                }
            break;

            case 1:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        //首尾也要做 Y / X 检查
                        if(judge(info->money[0], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        {
                            count = info->per_len_num[depth]++;
#ifdef NEON
                            memcpy_16(info->res4[count], info->path);
                            memcpy_16(info->res4[count] + 1, &info->three[i].u);
                            
                            info->char_len_num[1] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[1] += C_len[info->path[0]];
#else                      
                            memcpy(info->res4[count], info->path, depth << 2);
                            info->res4[count][1] = cur_id;
                            info->res4[count][2] = info->three[i].v;
                            info->res4[count][3] = info->three[i].k;
                            
                            info->char_len_num[1] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[1] += C_len[info->path[0]];
#endif
                        }
                    }
                }
                break;

            case 2:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        //if(judge(info->money[0], info->money[1]) && judge(info->money[1], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        if(judge(info->money[1], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        {
                            count = info->per_len_num[depth]++;
#ifdef NEON                            
                            memcpy_16(info->res5[count], info->path);
                            memcpy_16(info->res5[count] + 2, &info->three[i].u);

                            info->char_len_num[2] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[2] += C_len[info->path[0]] + C_len[info->path[1]];
#else                        
                            memcpy(info->res5[count], info->path, depth << 2);
                            info->res5[count][2] = cur_id;
                            info->res5[count][3] = info->three[i].v;
                            info->res5[count][4] = info->three[i].k;

                            info->char_len_num[2] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[2] += C_len[info->path[0]] + C_len[info->path[1]];
#endif
                        }
                    }
                }
                break;

            case 3:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                        //if(judge(info->money[0], info->money[1]) && judge(info->money[1], info->money[2]) && judge(info->money[2], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        if(judge(info->money[2], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        {
                            count = info->per_len_num[depth]++;
#ifdef NEON
                            memcpy_16(info->res6[count], info->path);
                            memcpy_16(info->res6[count] + 3, &info->three[i].u);
                            
                            info->char_len_num[3] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[3] += C_len[info->path[0]] + C_len[info->path[1]] + C_len[info->path[2]];
#else                 
                            memcpy(info->res6[count], info->path, depth << 2);
                            info->res6[count][3] = cur_id;
                            info->res6[count][4] = info->three[i].v;
                            info->res6[count][5] = info->three[i].k;

                            info->char_len_num[3] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[3] += C_len[info->path[0]] + C_len[info->path[1]] + C_len[info->path[2]];
#endif
                        }
                    }
                }
                break;

            case 4:
                for (Uint i = info->reachable[cur_id] - 1; info->three[i].u == cur_id; ++i)
                {
                    if (!info->visited[info->three[i].v] && !info->visited[info->three[i].k])
                    {
                                                
                        //if(judge(info->money[0], info->money[1]) && judge(info->money[1], info->money[2]) && judge(info->money[2], info->money[3]) && judge(info->money[3], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        if(judge(info->money[3], info->three[i].X) && judge(info->three[i].Y, info->money[0]))
                        {
                            count = info->per_len_num[depth]++;
#ifdef NEON                            
                            memcpy_16(info->res7[count], info->path);
                            memcpy_16(info->res7[count] + 4, &info->three[i].u);

                            info->char_len_num[4] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[4] += C_len[info->path[0]] + C_len[info->path[1]] + C_len[info->path[2]] + C_len[info->path[3]];
#else                            
                            memcpy(info->res7[count], info->path, depth << 2);
                            info->res7[count][4] = cur_id;
                            info->res7[count][5] = info->three[i].v;
                            info->res7[count][6] = info->three[i].k;

                            
                            info->char_len_num[4] += C_len[info->three[i].u] + C_len[info->three[i].v] + C_len[info->three[i].k];
                            info->char_len_num[4] += C_len[info->path[0]] + C_len[info->path[1]] + C_len[info->path[2]] + C_len[info->path[3]];
#endif                            
                        }

                    }
                }
                break;
            default:
                break;
        }
    }

    if (depth < 4)
    {
        if(!suc_index[cur_id][0])
            return;

        int begin = suc_index[cur_id][0] - 1, max_index = begin + suc_index[cur_id][1];

        while (begin <= max_index && start_id >= graph_suc[begin].id2)
            ++begin;

        info->visited[cur_id] = true;
        info->path[depth] = cur_id;
        
    
        for (; begin<= max_index; ++begin)
        {
            info->money[depth] = graph_suc[begin].money;
            if (!info->visited[graph_suc[begin].id2] && (!depth || judge(info->money[depth - 1], info->money[depth])))
            //if (!info->visited[G_suc[cur_id][begin].node])
            {
                dfs(start_id, graph_suc[begin].id2, depth + 1, info);
            }
        }
        info->visited[cur_id] = false;            
    }
}

inline bool cmp(Three &a, Three &b)
{
    return (a.u != b.u) ? a.u < b.u : (a.v != b.v ? a.v < b.v : a.k < b.k);
}

void* thread_dfs(void *arg)
{
    Result* info = (Result*)arg;
    int i, right = info->right;
    
    for(i = info->left; i < right; ++i)
    {
        pre_dfs(i, i , 0, info);

        if(info->three_len)
        {
            sort(info->three, info->three + info->three_len, cmp);
            info->three[info->three_len] = {INT_MAX, INT_MAX, INT_MAX, INT_MAX, INT_MAX};

            for(Uint j = 0; j < info->three_len; ++j)
            {
                if(!info->reachable[info->three[j].u])
                {
                    info->reachable[info->three[j].u] = j + 1;
                    info->currentJs[info->num_currentJS++] = info->three[j].u;
                }
            }
        }
        
        dfs(i, i, 0, info);

        for(int k = 0; k < info->num_currentJS; ++k)
        {
            info->reachable[info->currentJs[k]] = 0;
        }
        info->num_currentJS = 0;
        info->three_len = 0;
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

    const Uint length = digits10(value);

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

void save_result(char *resultFile)
{
#ifdef TEST
    clock_t start = clock();
#endif
    FILE *fp = fopen(resultFile, "w");
    // char *res_str = (char *)malloc(256 * 1024 * 1024);

    int count_loop = 0;

    for(int i = 0; i < THREAD_NUM; ++i)
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
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res3 + infos[k].per_len_num[0] * 3, 0);
        for(i = 0; i < infos[k].per_len_num[0]; ++i)
        {
            
            for(j = 0; j < 2; ++j)
            {
                index = infos[k].res3[i][j];
#ifdef NEON
                memcpy_16(res_str + res_len, Comma + (index << 3) + (index << 1) + index);
#else
                memcpy(res_str + res_len, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res3[i][2];
#ifdef NEON
            memcpy_16(res_str + res_len, LF + (index << 3) + (index << 1) + index);
#else
            memcpy(res_str + res_len, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
            res_len += C_len[index];
            // res_str[res_len++] = '\n';
        }
    }
    //len 4
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res4 + infos[k].per_len_num[1] * 4, 0);
        for(i = 0; i < infos[k].per_len_num[1]; ++i)
        {
            for(j = 0; j < 3; ++j)
            {
                index = infos[k].res4[i][j];
#ifdef NEON
                memcpy_16(res_str + res_len, Comma + (index << 3) + (index << 1) + index);
#else
                memcpy(res_str + res_len, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res4[i][3];
#ifdef NEON
            memcpy_16(res_str + res_len, LF + (index << 3) + (index << 1) + index);
#else
            memcpy(res_str + res_len, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
            res_len += C_len[index];
            // res_str[res_len++] = '\n';

        }
    }
    //len 5
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res5 + infos[k].per_len_num[2] * 5, 0);
        for(i = 0; i < infos[k].per_len_num[2]; ++i)
        {
            for(j = 0; j < 4; ++j)
            {
                index = infos[k].res5[i][j];
#ifdef NEON
                memcpy_16(res_str + res_len, Comma + (index << 3) + (index << 1) + index);
#else
                memcpy(res_str + res_len, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res5[i][4];
#ifdef NEON
            memcpy_16(res_str + res_len, LF + (index << 3) + (index << 1) + index);
#else
            memcpy(res_str + res_len, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
            res_len += C_len[index];
            // res_str[res_len++] = '\n';

        }
    }
    // len 6
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res6 + infos[k].per_len_num[3] * 6, 0);
        for(i = 0; i < infos[k].per_len_num[3]; ++i)
        {
            for(j = 0; j < 5; ++j)
            {
                index = infos[k].res6[i][j];
#ifdef NEON
                memcpy_16(res_str + res_len, Comma + (index << 3) + (index << 1) + index);
#else
                memcpy(res_str + res_len, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res6[i][5];
#ifdef NEON
            memcpy_16(res_str + res_len, LF + (index << 3) + (index << 1) + index);
#else
            memcpy(res_str + res_len, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
            res_len += C_len[index];
            // res_str[res_len++] = '\n';
        }
    }
    //len 7
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res7 + infos[k].per_len_num[4] * 7, 0);
        for(i = 0; i < infos[k].per_len_num[4]; ++i)
        {
            for(j = 0; j < 6; ++j)
            {
                index = infos[k].res7[i][j];
#ifdef NEON
                memcpy_16(res_str + res_len, Comma + (index << 3) + (index << 1) + index);
#else
                memcpy(res_str + res_len, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                res_len += C_len[index];
                // res_str[res_len++] = ',';
            }
            index = infos[k].res7[i][6];
#ifdef NEON
            memcpy_16(res_str + res_len, LF + (index << 3) + (index << 1) + index);
#else
            memcpy(res_str + res_len, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
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

void ThreadWrite3(int offset)
{
    int k,j,i,index;
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res3 + infos[k].per_len_num[0] * 3, 0);
        for(i = 0; i < infos[k].per_len_num[0]; ++i)
        {
            for(j = 0; j < 2; ++j)
            {
                index = infos[k].res3[i][j];
// #ifdef NEON
//                 memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
// #else
                memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
                offset += C_len[index];
            }
            index = infos[k].res3[i][2];
// #ifdef NEON
//             memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
// #else
            memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
            offset += C_len[index];
        }
    }
}
void ThreadWrite4(int offset)
{
    int i, j, k, index;
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res4 + infos[k].per_len_num[1] * 4, 0);
        for(i = 0; i < infos[k].per_len_num[1]; ++i)
        {
            for(j = 0; j < 3; ++j)
            {
                index = infos[k].res4[i][j];
// #ifdef NEON
//                 memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
// #else
                memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
                offset += C_len[index];
            }
            index = infos[k].res4[i][3];
// #ifdef NEON
//             memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
// #else
            memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
            offset += C_len[index];
        }
    }
}
void ThreadWrite5(int offset)
{
    int i,j,k,index;
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res5 + infos[k].per_len_num[2] * 5, 0);
        for(i = 0; i < infos[k].per_len_num[2]; ++i)
        {
            for(j = 0; j < 4; ++j)
            {
                index = infos[k].res5[i][j];
// #ifdef NEON
//                 memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
// #else
                memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
                offset += C_len[index];
            }
            index = infos[k].res5[i][4];
// #ifdef NEON
//             memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
// #else
            memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
            offset += C_len[index];
        }
    }
}
void ThreadWrite6(int offset)
{
    int i, j, k, index;
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res6 + infos[k].per_len_num[3] * 6, 0);
        for(i = 0; i < infos[k].per_len_num[3]; ++i)
        {
            for(j = 0; j < 5; ++j)
            {
                index = infos[k].res6[i][j];
// #ifdef NEON
                // memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
// #else
                memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
                offset += C_len[index];
            }
            index = infos[k].res6[i][5];
// #ifdef NEON
//             memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
// #else
            memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
            offset += C_len[index];
        }
    }
}
void ThreadWrite7(int offset)
{
    int i,j,k,index;
    for(k = 0; k < THREAD_NUM; k++)
    {
        //__builtin_prefetch(infos[k].res7 + infos[k].per_len_num[4] * 7, 0);
        for(i = 0; i < infos[k].per_len_num[4]; ++i)
        {
            for(j = 0; j < 6; ++j)
            {
                index = infos[k].res7[i][j];
// #ifdef NEON
//                 memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
// #else
                memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
                offset += C_len[index];
            }
            index = infos[k].res7[i][6];
// #ifdef NEON
//             memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
// #else
            memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
// #endif
            offset += C_len[index];
        }
    }
    res_str[offset] = '\0';
}

void ThreadWrite(int th_id, int offset)
{
    int i, j, k, index;
    switch(th_id)
    {
        case 0:
            for(k = 0; k < THREAD_NUM; k++)
            {
                //__builtin_prefetch(infos[k].res3 + infos[k].per_len_num[0] * 3, 0);
                for(i = 0; i < infos[k].per_len_num[0]; ++i)
                {
                    for(j = 0; j < 2; ++j)
                    {
                        index = infos[k].res3[i][j];
#ifdef NEON
                        memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
#else
                        memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                        offset += C_len[index];
                    }
                    index = infos[k].res3[i][2];
#ifdef NEON
                    memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
#else
                    memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                    offset += C_len[index];
                }
            }
            break;
        
        
        case 1:
            for(k = 0; k < THREAD_NUM; k++)
            {
                //__builtin_prefetch(infos[k].res4 + infos[k].per_len_num[1] * 4, 0);
                for(i = 0; i < infos[k].per_len_num[1]; ++i)
                {
                    for(j = 0; j < 3; ++j)
                    {
                        index = infos[k].res4[i][j];
#ifdef NEON
                        memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
#else
                        memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                        offset += C_len[index];
                    }
                    index = infos[k].res4[i][3];
#ifdef NEON
                    memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
#else
                    memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                    offset += C_len[index];
                }
            }
            break;

            
        case 2:
            for(k = 0; k < THREAD_NUM; k++)
            {
                //__builtin_prefetch(infos[k].res5 + infos[k].per_len_num[2] * 5, 0);
                for(i = 0; i < infos[k].per_len_num[2]; ++i)
                {
                    for(j = 0; j < 4; ++j)
                    {
                        index = infos[k].res5[i][j];
#ifdef NEON
                        memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
#else
                        memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                        offset += C_len[index];
                    }
                    index = infos[k].res5[i][4];
#ifdef NEON
                    memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
#else
                    memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                    offset += C_len[index];
                }
            }
            break;

            
        case 3:
            for(k = 0; k < THREAD_NUM; k++)
            {
                //__builtin_prefetch(infos[k].res6 + infos[k].per_len_num[3] * 6, 0);
                for(i = 0; i < infos[k].per_len_num[3]; ++i)
                {
                    for(j = 0; j < 5; ++j)
                    {
                        index = infos[k].res6[i][j];
#ifdef NEON
                        memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
#else
                        memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                        offset += C_len[index];
                    }
                    index = infos[k].res6[i][5];
#ifdef NEON
                    memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
#else
                    memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                    offset += C_len[index];
                }
            }
            break;

        case 4:
            for(k = 0; k < THREAD_NUM; k++)
            {
                //__builtin_prefetch(infos[k].res7 + infos[k].per_len_num[4] * 7, 0);
                for(i = 0; i < infos[k].per_len_num[4]; ++i)
                {
                    for(j = 0; j < 6; ++j)
                    {
                        index = infos[k].res7[i][j];
#ifdef NEON
                        memcpy_16(res_str + offset, Comma + (index << 3) + (index << 1) + index);
#else
                        memcpy(res_str + offset, Comma + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                        offset += C_len[index];
                    }
                    index = infos[k].res7[i][6];
#ifdef NEON
                    memcpy_16(res_str + offset, LF + (index << 3) + (index << 1) + index);
#else
                    memcpy(res_str + offset, LF + (index << 3) + (index << 1) + index, C_len[index]);
#endif
                    offset += C_len[index];
                }
            }
            res_str[offset] = '\0';
            break;
    }
}



void sortPre()
{
    sort(graph_suc, graph_suc + edge_num, suc_cmp);
}

void sortSuc()
{
    sort(graph_pre, graph_pre + edge_num, pre_cmp);
}

void sortID()
{
    sort(ID, ID + edge_num * 2);
    removeDuplicates();
    for(int n = 0; n < node_num; n++)
    {
        id_hash[ID[n]] = n;
        C_len[n] = u32ToAscii(ID[n], Comma  + n * 11, LF + n * 11);
    }
    // node_num = unique(ID, ID + edge_num * 2) - ID;
}

void createGraphPre()
{
    
    for(int k = 0; k < edge_num; ++k)
    {
        graph_pre[k] = Edge(id_hash[graph_pre[k].id1], id_hash[graph_pre[k].id2], graph_pre[k].money);
    }
    //for(int k = 0; k < edge_num; ++k)
    //{
    //    if(!pre_index[graph_pre[k].id1])
    //        pre_index[graph_pre[k].id1] = k + 1;
    //}
}

void createGraphSuc()
{
    
    for(int m = 0; m < edge_num; ++m)
    {
        graph_suc[m] = Edge(id_hash[graph_suc[m].id1], id_hash[graph_suc[m].id2], graph_suc[m].money);
    }
    //for(int m = 0; m < edge_num; ++m)
    //{
    //    if(!suc_index[graph_suc[m].id2])
    //        suc_index[graph_suc[m].id2] = m + 1;
    //}
}


int main(int argc, char *argv[])
{


/*DataSet 19630345 2408026 2541581 std 294 34195 41 51532 639096 697518 881420*/
#ifdef TEST
    
    string DataSet = "DataSet/";
    string test_data = "/test_data.txt";
    string my_ans = "/my_ans.txt";
    string input = DataSet + string(argv[1]) + test_data;
    string output = DataSet + string(argv[1]) + my_ans;
    char InputFile[100];
    char OutputFile[100];
    strcpy(InputFile, input.c_str());
    strcpy(OutputFile, output.c_str());
#else
    char InputFile[] = "/data/test_data.txt";
    char OutputFile[] = "/projects/student/result.txt";
#endif

// 读数据
#ifdef LINUX_MMAP

    read_data_mmap(InputFile);
#else
    read_data_fscanf(InputFile);
#endif
    
    // sort(graph_suc, graph_suc + edge_num, suc_cmp);
    // sort(graph_pre, graph_pre + edge_num, pre_cmp);

    // 合并 产生 ID

    //  sort(ID, ID + edge_num * 2);
    //  removeDuplicates();

    thread sortpreThread(sortPre);
    thread sortsucThread(sortSuc);
    thread sortidThread(sortID);
    
    sortpreThread.join();
    sortsucThread.join();
    sortidThread.join();
    // removeDuplicates();
    
    
    // 稍后考虑要不要循环展开
    // for(i = 0; i < node_num; i++)
    // {
    //      id_hash[ID[i]] = i;
    //      C_len[i] = u32ToAscii(ID[i], Comma  + i * 11, LF + i * 11);
    // }

    thread graphPreThread(createGraphPre);
    thread graphSucThread(createGraphSuc);

    graphPreThread.join();
    graphSucThread.join();

    // for(i = 0; i < edge_num; ++i)
    // {
    //      graph_suc[i] = Edge(id_hash[graph_suc[i].id1], id_hash[graph_suc[i].id2], graph_suc[i].money);
    //      graph_pre[i] = Edge(id_hash[graph_pre[i].id1], id_hash[graph_pre[i].id2], graph_pre[i].money);
    // }
    
    // register int i;
    // 记录 每个顶点在 graph_suc, graph_pre 的起始 index 位置
    for(int i = 0; i < edge_num; ++i)
    {
        if(!suc_index[graph_suc[i].id1][0])
            suc_index[graph_suc[i].id1][0] = i + 1;
        else
            suc_index[graph_suc[i].id1][1]++;

        if(!pre_index[graph_pre[i].id2][0])
            pre_index[graph_pre[i].id2][0] = i + 1;
        else
            pre_index[graph_pre[i].id2][1]++;
        
    }



    // 找环 (多线程) 后期换成 std::thread 减少代码量

    thread threads[THREAD_NUM];

    // pthread_t threads[THREAD_NUM];
    // pthread_attr_t attr;
    // void *status;

    // pthread_attr_init(&attr);
    // pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
   
    // int w = node_num / THREAD_NUM;

    // for(int i = 0; i < THREAD_NUM; i++)
    // {
	//     infos[i].left = w * i;
	//     infos[i].right = w * (i + 1);
    // }
    // infos[THREAD_NUM - 1].right = node_num;

    for(int i = 0; i < THREAD_NUM; i++)
    {
        infos[i].left = (int)(pthread_ratio[i] * node_num);
        infos[i].right = (int)(pthread_ratio[i + 1] * node_num);
    }

    for(int i = 0; i < THREAD_NUM; i++)
    {
        threads[i] = thread(thread_dfs, (void*)&(infos[i]));
    }
    for(auto& t: threads)
    {
        t.join();
    }
    // for(i = 0; i < THREAD_NUM; i++)
    // {
    //     int rc = pthread_create(&threads[i], NULL, thread_dfs, (void*)&(infos[i]));
    //     if(rc)
    //     {
    //         cout<<"pthread create error\n";
    //         exit(-1);
    //     }
    // }

    // pthread_attr_destroy(&attr);
    // for(i = 0; i < THREAD_NUM; ++i)
    // {
    //     int rc = pthread_join(threads[i], &status);
    //     if(rc)
    //     {
    //         cout<<"pthread join error\n";
    //         exit(-1);
    //     }
    // }
    // 写结果
    // save_result(OutputFile);
    
    // 多线程写
    FILE *fp = fopen(OutputFile, "w");
    int count_loop = 0;
    // register int j;
    for(int i = 0; i < THREAD_NUM; ++i)
    {
        for(int j = 0; j < 5; ++j)
        {
            count_loop += infos[i].per_len_num[j];
        } 
    }
#ifdef TEST
    printf("find %d Loops\n",count_loop); 
#endif
    int res_len = u32ToAscii_v2(count_loop, res_str);
    res_str[res_len++] = '\n';

    
    for(int i = 0; i < THREAD_NUM; ++i)
    {
        for(int j = 0; j < 5; ++j)
        {
            offset[j] += infos[i].char_len_num[j];
        } 
    }

    int a[5] = {0};
    for(int i = 0; i < 5; i++)
    {
         res_len += (i==0 ? 0 : offset[i - 1]);
         a[i] = res_len;
    }

    thread write3(ThreadWrite3, a[0]);
    thread write4(ThreadWrite4, a[1]);
    thread write5(ThreadWrite5, a[2]);
    thread write6(ThreadWrite6, a[3]);
    thread write7(ThreadWrite7, a[4]);

    write3.join();
    write4.join();
    write5.join();
    write6.join();
    write7.join();

    //thread writeThread[5];
    //for(int i = 0; i < 5; ++i)
    //{
    //    res_len += (i==0 ? 0 : offset[i - 1]);
    //    writeThread[i] = thread(ThreadWrite, i, res_len);
    //}

    //for(auto& t : writeThread)
    //{
    //    t.join();
    //}

    fwrite(res_str, sizeof(char), a[4] + offset[4], fp);
    fclose(fp);
//#ifdef TEST
    //cout<<"Well Done !\n";
//#endif    
    return 0;
}
