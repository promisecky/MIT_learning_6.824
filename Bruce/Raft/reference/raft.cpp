
#include "locker.h"
#include "Raft.h"
using namespace std;

#define COMMOM_PORT 1234
#define HEART_BEART_PERIOD 100000

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("loss parameter of peersNum\n");
        exit(-1);
    }
    int peersNum = atoi(argv[1]);
    if (peersNum % 2 == 0)
    {
        printf("the peersNum should be odd\n"); // 必须传入奇数，这是raft集群的要求
        exit(-1);
    }
    srand((unsigned)time(NULL));
    vector<PeersInfo> peers(peersNum);
    for (int i = 0; i < peersNum; i++)
    {
        peers[i].m_peerId = i;
        peers[i].m_port.first = COMMOM_PORT + i;                 // vote的RPC端口
        peers[i].m_port.second = COMMOM_PORT + i + peers.size(); // append的RPC端口
        // printf(" id : %d port1 : %d, port2 : %d\n", peers[i].m_peerId, peers[i].m_port.first, peers[i].m_port.second);
    }

    Raft *raft = new Raft[peers.size()];
    for (int i = 0; i < peers.size(); i++)
    {
        raft[i].Make(peers, i);
    }

    //------------------------------test部分--------------------------
    usleep(400000);
    for (int i = 0; i < peers.size(); i++)
    {
        if (raft[i].getState().second)
        {
            for (int j = 0; j < 1000; j++)
            {
                Operation opera;
                opera.op = "put";
                opera.key = to_string(j);
                opera.value = to_string(j);
                raft[i].start(opera);
                usleep(50000);
            }
        }
        else
            continue;
    }
    usleep(400000);
    for (int i = 0; i < peers.size(); i++)
    {
        if (raft[i].getState().second)
        {
            raft[i].kill(); // kill后选举及心跳的线程会宕机，会产生新的leader，很久之后了，因为上面传了1000条日志
            break;
        }
    }
    //------------------------------test部分--------------------------
    while (1)
        ;
}