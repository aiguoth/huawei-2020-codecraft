// #define TEST
#define MMAP

#include <stdio.h>
#include <string.h>
#include <bits/stdc++.h>
#include <queue>
#include <limits.h>
#include <sys/time.h>

#ifdef MMAP
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#endif

#include <ext/pb_ds/hash_policy.hpp>
#include <ext/pb_ds/assoc_container.hpp>

#include <thread>
#include <mutex>
using namespace std;

typedef unsigned int Uint;
typedef unsigned int ui;

const int MAX_EDGE_NUM = 2500000;
const int MAX_NODE_NUM = 2500000;
const int THREAD_NUM = 12;
const int TOP_NUM = 100; // TopN

int edge_num = 0, node_num = 0, u_num = 0, v_num = 0;

// u -[w]-> v
Uint U[MAX_EDGE_NUM];
Uint V[MAX_EDGE_NUM];
Uint W[MAX_EDGE_NUM];


Uint new_U[MAX_EDGE_NUM];
Uint new_V[MAX_EDGE_NUM];
Uint new_W[MAX_EDGE_NUM];

Uint U_temp[MAX_EDGE_NUM]; // 相当于题目中的 S 集合
Uint V_temp[MAX_EDGE_NUM]; // 相当于题目中的 T 集合

Uint id[MAX_EDGE_NUM * 2]; // 排序 + 去重
Uint new_id[MAX_NODE_NUM * 2]; //之后的新映射

Uint outDegree[MAX_EDGE_NUM];
Uint inDegree[MAX_EDGE_NUM];
Uint newOutDegree[MAX_EDGE_NUM]; 
Uint newInDegree[MAX_EDGE_NUM];

//链式前向星中的 head[], edge[].next;
Uint U_head[MAX_EDGE_NUM];
Uint V_head[MAX_EDGE_NUM];
Uint U_next[MAX_EDGE_NUM];
Uint V_next[MAX_EDGE_NUM];

Uint new_U_head[MAX_EDGE_NUM];
Uint new_V_head[MAX_EDGE_NUM];
Uint new_U_next[MAX_EDGE_NUM];
Uint new_V_next[MAX_EDGE_NUM];


Uint topo_Head[MAX_NODE_NUM];

Uint del_node[MAX_NODE_NUM];


bool del_flag[MAX_NODE_NUM];

Uint topo_pred_info[MAX_NODE_NUM][2];

Uint del_pred_num[MAX_NODE_NUM];

bool vis[MAX_NODE_NUM];

Uint cur_s = 0; // 多线程 任务片划分 当前最大分配的 s 
mutex s_lock;

double CB[MAX_NODE_NUM]; //保存最终的 CB(i)



// 线下测试速度 gp_hash_table 比 unordered_map 快
__gnu_pbds::gp_hash_table<Uint, int> id_hash;
__gnu_pbds::gp_hash_table<Uint, int> new_id_hash;

Uint graph_suc[MAX_EDGE_NUM][2]; // id2 | distance


struct HashInfo
{
    Uint id;
    Uint hash_id;

    inline HashInfo(Uint id, Uint hash_id) : id(id), hash_id(hash_id) {}
};


struct Q_queue
{
    Uint node;
    Uint distance;

    Q_queue() {}
    Q_queue(Uint node, Uint distance) : node(node), distance(distance) {}

    bool operator<(const Q_queue &rhs) const
    {
        return distance > rhs.distance;
    }
};



struct ThreadInfo
{
    Uint S[MAX_NODE_NUM]; // 用数组模拟 paper 伪代码中的 stack
    Uint d[MAX_NODE_NUM];
    Uint sigma[MAX_NODE_NUM];

    priority_queue<Q_queue> Q;    //最小堆

    // double delta[MAX_NODE_NUM];
    // double partCB[MAX_NODE_NUM];    // 保存该线程下计算所得的 部分 CB(i)

    double delta_cb[MAX_NODE_NUM][2];

    Uint pred_next_ptr[MAX_NODE_NUM];
    Uint pred_info[MAX_NODE_NUM];

}TInfo[THREAD_NUM];

struct Result
{
    Uint node;
    double cb;

    Result() {}

    Result(Uint node, double cb) : node(node), cb(cb) {}

    bool operator<(const Result &rhs) const
    {
        // if (abs(cb - rhs.cb) > 0.0001)
        //     return cb > rhs.cb;
        // else
        //     return new_id[id] < new_id[rhs.id];
        return (abs(cb - rhs.cb) > 1e-4) ? cb > rhs.cb : new_id[node] < new_id[rhs.node];
    }
};


#ifdef TEST
// 计时器
struct Time_recorder
{
    vector<pair<string, int>> logs;
    struct timeval start_time;
    struct timeval end_time;

    void setTime()
    {
        gettimeofday(&start_time, NULL);
    }

    int getElapsedTimeMS()
    {
        gettimeofday(&end_time, NULL);
        return int(1000 * (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) / 1000);
    }

    void logTimeImpl(string tag, int elapsedTimeMS)
    {
        printf("Time consumed for %-20s is %d ms.\n", tag.c_str(), elapsedTimeMS);
    }

    void logTime(string tag)
    {
        logTimeImpl(tag, getElapsedTimeMS());
    }

    void resetTimeWithTag(string tag)
    {
        logs.emplace_back(make_pair(tag, getElapsedTimeMS()));
        setTime();
    }

    void printLogs()
    {
        for (auto x : logs)
            logTimeImpl(x.first, x.second);
    }
};
#endif

#ifdef MMAP
void readDataWithMmap(char* inputFile)
{
    struct stat stat_buf;
    int fd = open(inputFile, O_RDONLY);
    
    // fseek稍微慢一点
    fstat(fd, &stat_buf);
    register long length = stat_buf.st_size;
    char *mbuf = (char *)mmap(NULL, length, PROT_READ, MAP_PRIVATE, fd, 0);
    register Uint num = 0;
    register int flag = 0;
    register long index = 0;
    register char ch;
    while(index < length)
    {
        ch = *(mbuf + index);
        if(ch >= '0')
        {
            num = num * 10 + ch - '0';
        }
        else if(ch != '\n')
        {
            switch (flag)
            {
            case 0:
                U[edge_num] = num;
                flag = 1, num = 0;
                break;
            case 1:
                V[edge_num] = num;
                flag = 2, num = 0;
                break;
            case 2:
                W[edge_num++] = num;
                flag = 0, num = 0;
                break;
            }
        }
        ++index;
    }
    close(fd);
    munmap(mbuf, length);
}
#endif

void dfs(Uint cur_id, Uint depth, Uint num, Uint tid)
{
    Uint pred_id, pred_num;

    auto &delta_cb = TInfo[tid].delta_cb;

    for (Uint i = topo_Head[cur_id]; i != 0; i = topo_pred_info[i - 1][1])
    {
        pred_id = topo_pred_info[i - 1][0];
        pred_num = del_pred_num[pred_id] + 1;
        // TInfo[tid].partCB[cur_id] += pred_num * (num + depth);

        delta_cb[cur_id][1] += pred_num * (num + depth);
        
        if (pred_num > 1)
            dfs(pred_id, depth + 1, num, tid);
    }
}

//paper : A Faster Algorithm for Betweenness Centrality
void dijkstra_heap_opt(Uint s, Uint tid)
{
    //不用每次都写 TInfo[tid]. 了
    auto &d = TInfo[tid].d;
    auto &sigma = TInfo[tid].sigma;
    auto &pred_next_ptr = TInfo[tid].pred_next_ptr;
    auto &Q = TInfo[tid].Q;
    auto &S = TInfo[tid].S;
    auto &pred_info = TInfo[tid].pred_info;

    auto &delta_cb = TInfo[tid].delta_cb;
    // auto &partCB = TInfo[tid].partCB;
    // auto &delta = TInfo[tid].delta;
 
    int s_index = -1; // 记录 Stack S
    Uint multiple, pred_info_len = 0;

    double coeff;

    Uint v, w;
    Uint v_dis;
    Uint j_begin, j_end; //j 变量图的指针 范围[ j_begin, j_end)

    d[s] = 0;
    sigma[s] = 1;

    Q.emplace(Q_queue(s, 0));

    while (!Q.empty())
    {
        v_dis = Q.top().distance;
        v = Q.top().node;
        Q.pop();

        if (v_dis > d[v])
            continue;

        S[++s_index] = v;

        // delta[v] = 0;
        delta_cb[v][0] = 0;

        j_begin = new_U_head[v];
        j_end = j_begin + newOutDegree[v];
        while (j_begin < j_end)
        {
            w = graph_suc[j_begin][0];

            if (d[v] + graph_suc[j_begin][1] == d[w])
            {
                sigma[w] += sigma[v];

                pred_info[pred_next_ptr[w]++] = v;
            }
            else if (d[v] + graph_suc[j_begin][1] < d[w])
            {
                d[w] = d[v] + graph_suc[j_begin][1];
                Q.emplace(Q_queue(w, d[w]));

                sigma[w] = sigma[v];

                pred_next_ptr[w] = new_V_head[w];
                pred_info[pred_next_ptr[w]++] = v;
            }
            ++j_begin;
        }
    }

    multiple = del_pred_num[s] + 1; //计算删除前驱数量
    dfs(s, 0, s_index, tid); //去计算这些删掉节点的 spartCB

    while (s_index > 0)
    {
        w = S[s_index--];
        // partCB[w] += delta[w] * multiple;

        delta_cb[w][1] += delta_cb[w][0] * multiple;
        
        d[w] = -1u;
        coeff = (1 + delta_cb[w][0]) / sigma[w];

        j_begin = new_V_head[w];
        j_end = pred_next_ptr[w];

        while (j_begin < j_end)
        {
            v = pred_info[j_begin++];
            delta_cb[v][0] += sigma[v] * coeff;
        }
    }
    d[s] = -1u;
}

void thread_process(Uint tid)
{
    auto &d = TInfo[tid].d;
    for (Uint i = 0; i < node_num; ++i)
    {
        d[i] = -1u;
    }
    
    Uint s;
    while (true)
    {
        s_lock.lock();
        if (cur_s < node_num)
        {
            while (del_flag[cur_s] && cur_s < node_num)
                cur_s++;

            if (cur_s < node_num)
                s = cur_s++;
            else
            {
                s_lock.unlock();
                break;
            }

            s_lock.unlock();
            dijkstra_heap_opt(s, tid);
        }
        else
        {
            s_lock.unlock();
            break;
        }
    }
}



void saveResult(char *outputFile)
{

    for (Uint i = 0; i < THREAD_NUM; ++i)
    {
        for (Uint j = 0; j < node_num; ++j)
        {
            // CB[j] += TInfo[i].partCB[j];
            CB[j] += TInfo[i].delta_cb[j][1];
        }
    }

    int i = 0;
    priority_queue<Result> res;
    while (i < TOP_NUM)         //放入TOP_NUM数量的结果，如果如果来的结果比 top()值大，则交换
    {
        res.emplace(Result(i, CB[i]));
        ++i;
    }

    while (i < node_num)
    {
        // if (CB[index] > res.top().score && abs(CB[index] - res.top().score) > 0.0001)
        if(CB[i] - res.top().cb > 0.0001)
        {
            res.pop();
            res.emplace(Result(i, CB[i]));
        }
        ++i;
    }

    FILE *fp = fopen(outputFile, "w");

    Result result[TOP_NUM];

    i = 99;
    while (i >= 0)
    {
        result[i] = res.top();
        res.pop();
        i--;
    }
    i = 0;
    while (i < 100)
    {
        fprintf(fp, "%u,%.3lf\n", id[new_id[result[i].node]], result[i].cb);
        ++i;
    }

    fclose(fp);
}

void sortAndUnique_u()
{
    memcpy(U_temp, U, sizeof(U));
    sort(U_temp, U_temp + edge_num);
    u_num = unique(U_temp, U_temp + edge_num) - U_temp;
}

void sortAndUnique_v()
{
    memcpy(V_temp, V, sizeof(V));
    sort(V_temp, V_temp + edge_num);
    v_num = unique(V_temp, V_temp + edge_num) - V_temp;
}

void mergeId()
{
    register int i = 0, j = 0;

    while(i < u_num && j < v_num)
    {
        if(U_temp[i] < V_temp[j])
        {
            id[node_num++] = U_temp[i++];
        }
        else if(U_temp[i] == V_temp[j])
        {
            id[node_num++] = U_temp[i++];
            j++;
        }
        else
        {
            id[node_num++] = V_temp[j++];
        }
    }
    while(i < u_num)
    {
        id[node_num++] = U_temp[i++];
    }

    while(j < v_num)
    {
        id[node_num++] = V_temp[j++];
    }
}

int main(int argc, char *argv[])
{

#ifdef TEST
    Time_recorder timeA;
    string dataSet = "DataSet/";
    string testData = "/test_data.txt"; 
    string myAns = "/myAnswer.txt";
    string input = dataSet + string(argv[1]) + testData;
    string output = dataSet + string(argv[1]) + myAns;
    
    char inputFile[100];
    char outputFile[100];

    strcpy(inputFile, input.c_str());
    strcpy(outputFile, output.c_str()); 
#else

    char inputFile[] = "/data/test_data.txt";
    char outputFile[] = "/projects/student/result.txt";

#endif

#ifdef TEST
    timeA.setTime();
#endif

    readDataWithMmap(inputFile);

#ifdef TEST
    timeA.resetTimeWithTag("Read Data");
#endif

    // 多线程 对 U, V排序去重，并合并到 id 中
    thread sortU(sortAndUnique_u);
    thread sortV(sortAndUnique_v);
    sortU.join();
    sortV.join();
    mergeId();

#ifdef TEST
    timeA.resetTimeWithTag("U,V Sort、Unique and Conquer");
#endif

    register int i;

    // hash 顶点与 index 的映射
    for(i = 0; i < node_num; ++i)
    {
        id_hash[id[i]] = i;
    }

    Uint u, v;
    Uint index = 0;

    while (index < edge_num)
    {
        // 将原来的顶点 换成 映射 index
        u = U[index] = id_hash[U[index]];
        v = V[index] = id_hash[V[index]];

        // 链式前向星
        U_next[index] = U_head[u];
        U_head[u] = ++index;

        //出入度
        outDegree[u]++;
        inDegree[v]++;
    }

//=============================================================================

    Uint cur_id = 0, cur_hash_id, next_id, hash_index = 0, u_hash_index;
    

    //删除点前需要重新映射
    
    i = 0;
    queue<HashInfo> q;
    while(true)
    {
        if(i == node_num)
            break;
        
        while (i < node_num && vis[i])
        {
            i++;
        }
        

        vis[i] = true;
        q.push(HashInfo(i, hash_index));
        hash_index++;

        while (!q.empty())
        {
            HashInfo hashdata = q.front();
            cur_id = hashdata.id;
            cur_hash_id = hashdata.hash_id;
            new_id_hash[cur_id] = cur_hash_id;
            new_id[cur_hash_id] = cur_id;
            u_hash_index = cur_hash_id;

            q.pop();
            for (Uint next_index = U_head[cur_id]; next_index != 0; next_index = U_next[next_index - 1])
            {
                next_id = V[next_index - 1];

                if (!vis[next_id])
                {
                    new_U[next_index - 1] = u_hash_index;
                    new_V[next_index - 1] = hash_index;

                    vis[next_id] = true;
                    new_id[hash_index] = next_id;
                    new_id_hash[next_id] = hash_index;
                    q.push(HashInfo(next_id, hash_index));
                    hash_index++;
                }
                else
                {
                    new_U[next_index - 1] = u_hash_index;
                    new_V[next_index - 1] = new_id_hash[next_id];
                }
            }
        }

    }

    for (i = 0; i < node_num; i++)
    {
        new_U_head[new_id_hash[i]] = U_head[i];
        new_V_head[new_id_hash[i]] = V_head[i];
        newOutDegree[new_id_hash[i]] = outDegree[i];
        newInDegree[new_id_hash[i]] = inDegree[i];
    }

#ifdef TEST
    timeA.resetTimeWithTag("New Hash Id");
#endif



    // “删除” 入度为0，出度为1的点
    int topo_index = -1;
    for (i = 0; i < node_num; ++i)
    {
        if (newInDegree[i] == 0 && newOutDegree[i] == 1)
        {
            del_node[++topo_index] = i;
            del_flag[i] = true;
        }
    }
    Uint count = 0;
    while (topo_index >= 0)
    {
        u = del_node[topo_index--];
        i = new_U_head[u];
        v = new_V[i - 1];

        topo_pred_info[count][0] = u;
        topo_pred_info[count][1] = topo_Head[v];
        topo_Head[v] = ++count;
        del_pred_num[v] += del_pred_num[u] + 1; //记录 v 删掉的前驱顶点的数量

        --newOutDegree[u];
        --newInDegree[v];
        //递归删除 所有满足入度为0，出度为1的点
        if (newInDegree[v] == 0 && newOutDegree[v] == 1)
        {
            del_node[++topo_index] = v;
            del_flag[v] = true;
        }
    }

#ifdef TEST
    timeA.resetTimeWithTag("Del 1/0 Node ");
#endif
    
    Uint j, suc_edge_num = 0;
    for(i = 0; i < node_num; ++i)
    {
        if(del_flag[i] == false)
        {
            for(j = new_U_head[i], new_U_head[i] = suc_edge_num; j != 0; j = U_next[j - 1])
            {
                    graph_suc[suc_edge_num][0] = new_V[j - 1];
                    graph_suc[suc_edge_num++][1] = W[j - 1];
            }
        }
    }
    new_U_head[i] = suc_edge_num;

    Uint pre_index = 0;
    for(i = 0; i < node_num; ++i)
    {
        new_V_head[i] = pre_index;
        pre_index += newInDegree[i];
    }

#ifdef TEST
    timeA.resetTimeWithTag("Create Graph and Pre Index");
#endif

    // 多线程 计算 CB(i)
    thread threads[THREAD_NUM];

    for(i = 0; i < THREAD_NUM; ++i)
    {
        threads[i] = thread(thread_process, i);
    }

    for(auto &t : threads)
    {
        t.join();
    }
    
#ifdef TEST
    timeA.resetTimeWithTag("Threads Compute CB");
#endif

    saveResult(outputFile);
#ifdef TEST
    timeA.resetTimeWithTag("Save Result");      
    timeA.printLogs();  
#endif

    return 0;
}