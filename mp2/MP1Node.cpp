/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    // int id = *(int*)(&memberNode->addr.addr);
    // int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up t he group
#ifdef DEBUGLOG
        cout << "Starting up group..." << endl;
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *) (msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        cout << memberNode->addr.getAddress() << " Trying to join..." << endl;
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *) msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    /*
     * Your code goes here
     */
    memberNode->bFailed = false;
    memberNode->inited = false;
    memberNode->inGroup = false;
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    memberNode->memberList.clear();
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    //get the messageType
    MessageHdr *msg;
    msg = (MessageHdr *) malloc(sizeof(MessageHdr));
    memcpy(msg, data, sizeof(MessageHdr));

    switch (msg->msgType) {
        case JOINREQ:
            return recvJOINREQ(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case JOINREP:
            return recvJOINREP(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case HEARTBEAT:
            return recvHeartBeat(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
    }
    return true;
}

bool MP1Node::recvJOINREQ(void *env, char *data, int size) {
//get the address and heartbeat
    Address *address;
    address = (Address *) malloc(sizeof(Address));
    memcpy(address->addr, data, sizeof(address->addr));
    // cout<<address->getAddress()<<endl;

    long heartbeat;
    memcpy(&heartbeat, data + sizeof(address->addr), sizeof(long));
    // cout<<heartbeat<<endl;

    int id = *(int *) (&address->addr);
    short port = *(short *) (&address->addr[4]);

    updateMemberList(id, port, heartbeat);

    sendMemberList(JOINREP, address);

    return false;
}

bool MP1Node::recvJOINREP(void *env, char *data, int size) {


    recvHeartBeat(env, data, size);

    //mark in group
    memberNode->inGroup = true;
//    cout<<(int)memberNode->addr.addr[0]<<" set self in group"<<endl;

    return false;
}

//if it's in the memberlist, update the heartbeat, else add it to the memberlist
void MP1Node::updateMemberList(int id, short port, long heartbeat) {
    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
        if (it->id == id && it->port == port) {
            if (it->getheartbeat() < heartbeat) {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
            }
            return;
        }
    }

//    cout << id << ":" << port << " try to join node " <<(int) memberNode->addr.addr[0]<<endl;

    //new member, add it to the memberlist
    if (id != *(int *) memberNode->addr.addr || port != (short) memberNode->addr.addr[4]) {
#ifdef DEBUGLOG
//            cout << id << ":" << port << " added to the group of " <<(int) memberNode->addr.addr[0]<<endl;
        Address joinedAddr;
        memcpy(&joinedAddr.addr[0], &id, sizeof(int));
        memcpy(&joinedAddr.addr[4], &port, sizeof(short));
        log->logNodeAdd(&memberNode->addr, &joinedAddr);
#endif
        MemberListEntry mle(id, port, heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(mle);
    }
}

//assemble a JOINREP message and sent it to joiner
void MP1Node::sendMemberList(enum MsgTypes msgType, Address *address) {
    long memberSize = memberNode->memberList.size();
    long dataSize = 0;
    char *memberListData;
    memberListData = (char *) malloc(memberSize * sizeof(MemberListEntry));

    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        if (par->getcurrtime() - it->timestamp > TREMOVE) {
#ifdef DEBUGLOG
            Address leaveAddr;
            memcpy(&leaveAddr.addr[0], &it->id, sizeof(int));
            memcpy(&leaveAddr.addr[4], &it->port, sizeof(short));
//            cout << "remove " << it->id << ":" << it->port << " from the group" << endl;
            log->logNodeRemove(&memberNode->addr, &leaveAddr);
#endif
            memberSize--;
            memberNode->memberList.erase(it);
            continue;
        } else if (par->getcurrtime() - it->timestamp > TFAIL) {
            memberSize--;
            it++;
            continue;
        } else {
            MemberListEntry mle(it->id, it->port, it->heartbeat, par->getcurrtime());
//            if (par->getcurrtime() < 20) {
//                cout << (int) memberNode->addr.addr[0] << " sent hearbeat to " << it->id << endl;
//            }
            memcpy(memberListData + dataSize * sizeof(MemberListEntry), &mle, sizeof(MemberListEntry));
            it++;
            dataSize++;
            continue;
        }
    }

    memberNode->heartbeat = par->getcurrtime();

    MessageHdr *rep;
    size_t msgSize = sizeof(MessageHdr) + sizeof(address->addr) + sizeof(long) * 2 + sizeof(MemberListEntry) * dataSize;
    rep = (MessageHdr *) malloc(msgSize * sizeof(char));
    rep->msgType = msgType;

    memcpy((char *) (rep + 1), memberNode->addr.addr, sizeof(address->addr));
    memcpy((char *) (rep + 1) + sizeof(address->addr), &dataSize, sizeof(long));
    memcpy((char *) (rep + 1) + sizeof(address->addr) + sizeof(long), &memberNode->heartbeat, sizeof(long));
    memcpy((char *) (rep + 1) + sizeof(address->addr) + sizeof(long) * 2, memberListData,
           sizeof(MemberListEntry) * dataSize);
    emulNet->ENsend(&memberNode->addr, address, (char *) rep, msgSize);
    free(rep);
}

void MP1Node::sendHeartBeat() {
    int targets;
    targets = (int) (rand() % memberNode->memberList.size());
//    if (par->getcurrtime() < 20) {
//        cout << (int) memberNode->addr.addr[0] << " member size is " << memberNode->memberList.size() << endl;
//    }
    for (int i = 0; i <= memberNode->memberList.size() / 2; i++) {
        Address toAdd;
        int id = memberNode->memberList[(i + targets) % memberNode->memberList.size()].getid();
        short port = memberNode->memberList[(i + targets) % memberNode->memberList.size()].getport();
        memcpy(&toAdd.addr[0], &id, sizeof(int));
        memcpy(&toAdd.addr[4], &port, sizeof(short));
        sendMemberList(HEARTBEAT, &toAdd);
    }
}

bool MP1Node::recvHeartBeat(void *env, char *data, int size) {
    Address *address;
    address = (Address *) malloc(sizeof(Address));
    memcpy(address->addr, data, sizeof(address->addr));

    int id = *(int *) (&address->addr);
    short port = *(short *) (&address->addr[4]);

    long memberListSize;
    memcpy(&memberListSize, data + sizeof(address->addr), sizeof(long));
//    if (par->getcurrtime() < 20) {
//        cout<<"memberListSize that "<<(int)memberNode->addr.addr[0]<<" received from "<<id<< " is "<<memberListSize<<endl;
//    }

    long heartbeat;
    memcpy(&heartbeat, data + sizeof(address->addr) + sizeof(long), sizeof(long));

    for (int i = 0; i < memberListSize; i++) {
        MemberListEntry *mle;
        mle = (MemberListEntry *) malloc(sizeof(MemberListEntry));
        memcpy(mle, data + sizeof(address->addr) + sizeof(long) * 2 + i * sizeof(MemberListEntry),
               sizeof(MemberListEntry));
//        cout<<(int)memberNode->addr.addr[0]<<" receive heartbeat from "<<id<<":"<<port<<" - "<<heartbeat<<endl;
        updateMemberList(mle->id, mle->port, mle->heartbeat);
    }
    updateMemberList(id, port, heartbeat);
//    cout<<(int)memberNode->addr.addr[0]<<" receive heartbeat from "<<id<<":"<<port<<" - "<<heartbeat<<endl;

    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    /*
     * Your code goes here
     */
    if (memberNode->memberList.size() > 0) {
        sendHeartBeat();
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *) &addr->addr[4]);
}
