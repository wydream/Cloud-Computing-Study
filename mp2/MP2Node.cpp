/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

map<int, Message *> sentMessages;
map<int, int> successReplies;
map<int, int> failedReplies;
map<int, long> readcounter;
map<int, long> updatecounter;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change_ring = false;
    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());
    // if MemList changed, update ring
    if (!sameRing(curMemList, ring)) {
        log->LOG(&memberNode->addr, "update the ring");
        ring = curMemList;
        change_ring = true;
        unsigned long pos = 0;
        vector<Node>::iterator it;
        for (it = ring.begin(); it != ring.end(); it++, pos++) {
            if (it->nodeHashCode == myHashCode) {
                haveReplicasOf.clear();
                haveReplicasOf.push_back(ring.at((pos - 2 + ring.size()) % ring.size()));
                haveReplicasOf.push_back(ring.at((pos - 1 + ring.size()) % ring.size()));
                hasMyReplicas.clear();
                hasMyReplicas.push_back(ring.at((pos + 1) % ring.size()));
                hasMyReplicas.push_back(ring.at((pos + 2) % ring.size()));
            }
        }
    }

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    /**
     * Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
     * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
     * 				It ensures that there always 3 copies of all keys in the DHT at all times
     * 				The function does the following:
     *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
     *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
     */
    if (ring.size() != 0 && change_ring && ht->hashTable.size() > 0) {
        map<string, string>::iterator it;
        for (it = ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
            Entry value = Entry(it->second);
            if (value.replica == PRIMARY) {
                Entry transValue1 = Entry(value.value, par->getcurrtime(), SECONDARY);
                Entry transValue2 = Entry(value.value, par->getcurrtime(), TERTIARY);

                Message message1 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue1.convertToString());
                Message message2 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue2.convertToString());

                emulNet->ENsend(&memberNode->addr, hasMyReplicas.at(0).getAddress(), (char *) &message1, sizeof(Message));
                emulNet->ENsend(&memberNode->addr, hasMyReplicas.at(1).getAddress(), (char *) &message2, sizeof(Message));
            } else if (value.replica == SECONDARY){
                Entry transValue1 = Entry(value.value, par->getcurrtime(), PRIMARY);
                Entry transValue2 = Entry(value.value, par->getcurrtime(), TERTIARY);

                Message message1 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue1.convertToString());
                Message message2 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue2.convertToString());

                emulNet->ENsend(&memberNode->addr, haveReplicasOf.at(1).getAddress(), (char *) &message1, sizeof(Message));
                emulNet->ENsend(&memberNode->addr, hasMyReplicas.at(0).getAddress(), (char *) &message2, sizeof(Message));
            } else if (value.replica == TERTIARY) {
                Entry transValue1 = Entry(value.value, par->getcurrtime(), PRIMARY);
                Entry transValue2 = Entry(value.value, par->getcurrtime(), SECONDARY);

                Message message1 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue1.convertToString());
                Message message2 = Message(g_transID++, memberNode->addr, READREPLY, it->first, transValue2.convertToString());

                emulNet->ENsend(&memberNode->addr, haveReplicasOf.at(0).getAddress(), (char *) &message1, sizeof(Message));
                emulNet->ENsend(&memberNode->addr, hasMyReplicas.at(1).getAddress(), (char *) &message2, sizeof(Message));
            }
        }
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    Node myNode = Node(memberNode->addr);
    this->myHashCode = myNode.nodeHashCode;
    curMemList.emplace_back(myNode);
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 */
void MP2Node::clientCreate(string key, string value) {
    buildClientMessages(key, value, CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 */
void MP2Node::clientRead(string key) {
    buildClientMessages(key, "", READ);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 */
void MP2Node::clientUpdate(string key, string value) {
    buildClientMessages(key, value, UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 */
void MP2Node::clientDelete(string key) {
    buildClientMessages(key, "", DELETE);
}

void MP2Node::buildClientMessages(string key, string value, MessageType type) {
    //1.Constructs the message
    int transID = g_transID++;
    Message message = Message(transID, memberNode->addr, type, key);
    successReplies[transID] = 0;
    failedReplies[transID] = 0;
    if (type == READ) {
        readcounter[transID] = par->getcurrtime();
    }
    if (type == UPDATE) {
        updatecounter[transID] = par->getcurrtime();
    }

    //2.Finds the replicas of this key
    vector<Node> replicas = findNodes(key);

    //3.Sends a message to the replica
    vector<Node>::iterator it;
    for (it = replicas.begin(); it != replicas.end(); it++) {
        if (it->getHashCode() == this->myHashCode) {
            // if it's local, call local function
            ReplicaType replicaType;
            if (myHashCode == replicas.at(0).getHashCode()) {
                replicaType = PRIMARY;
            } else if (myHashCode == replicas.at(1).getHashCode()) {
                replicaType = SECONDARY;
            } else {
                replicaType = TERTIARY;
            }
            Entry *entryValue = new Entry(value, par->getcurrtime(), replicaType);
            message.replica = replicaType;
            message.value = entryValue->convertToString();
            sentMessages[transID] = new Message(message);
            switch (type) {
                case CREATE: {
                    createKeyValue(&message);
                    break;
                }
                case READ: {
                    readKey(&message);
                    break;
                }
                case UPDATE: {
                    updateKeyValue(&message);
                    break;
                }
                case DELETE: {
                    deletekey(&message);
                    break;
                }
                default:
                    break;
            }
        } else {
            ReplicaType replicaType;
            if (it->getHashCode() == replicas.at(0).getHashCode()) {
                replicaType = PRIMARY;
            } else if (it->getHashCode() == replicas.at(1).getHashCode()) {
                replicaType = SECONDARY;
            } else {
                replicaType = TERTIARY;
            }
            Entry *entryValue = new Entry(value, par->getcurrtime(), replicaType);
            message.replica = replicaType;
            message.value = entryValue->convertToString();
            sentMessages[transID] = new Message(message);
            emulNet->ENsend(&memberNode->addr, it->getAddress(), (char *) &message, sizeof(Message));
        }
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(Message *message) {
    //do create
    bool result = ht->create(message->key, message->value);
    //log the result
    if (result) {
        log->logCreateSuccess(&memberNode->addr, false, message->transID, message->key, message->value);
    } else {
        log->logCreateFail(&memberNode->addr, false, message->transID, message->key, message->value);
    }
    //construct a reply and send it
    Message reply = Message(message->transID, memberNode->addr, REPLY, result);
    if (sameAddress(&memberNode->addr, &message->fromAddr)) {
        // if it's local, call local function
        handleReply(&reply);
    } else {
        emulNet->ENsend(&memberNode->addr, &message->fromAddr, (char *) &reply, sizeof(Message));
    }
    return result;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(Message *message) {
    //do read
    string value = ht->read(message->key);
    //log the result
    if (value.compare("") == 0) {
        log->logReadFail(&memberNode->addr, false, message->transID, message->key);
    } else {
        log->logReadSuccess(&memberNode->addr, false, message->transID, message->key, value);
    }
    //construct a reply and send it
    Message reply = Message(message->transID, memberNode->addr, REPLY, message->key, value);
    if (sameAddress(&memberNode->addr, &message->fromAddr)) {
        // if it's local, call local function
        handleReply(&reply);
    } else {
        emulNet->ENsend(&memberNode->addr, &message->fromAddr, (char *) &reply, sizeof(Message));
    }
    return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(Message *message) {
    //do update
    bool result = ht->update(message->key, message->value);
    //log the result
    if (result) {
        log->logUpdateSuccess(&memberNode->addr, false, message->transID, message->key, message->value);
    } else {
        log->logUpdateFail(&memberNode->addr, false, message->transID, message->key, message->value);
    }
    //construct a reply and send it
    Message reply = Message(message->transID, memberNode->addr, REPLY, result);
    if (sameAddress(&memberNode->addr, &message->fromAddr)) {
        // if it's local, call local function
        handleReply(&reply);
    } else {
        emulNet->ENsend(&memberNode->addr, &message->fromAddr, (char *) &reply, sizeof(Message));
    }
}

bool MP2Node::stabilization(Message *message) {
    //just do update
    bool result = false;
    string v = ht->read(message->key);
    if (v.empty()) {
        ht->create(message->key, message->value);
    } else {
        if (v.compare(message->value) == 0) {
            return result;
        } else {
            result = ht->update(message->key, message->value);
//            if (result) {
//                log->logUpdateSuccess(&memberNode->addr, false, message->transID, message->key, message->value + "-stabilization");
//            } else {
//                log->logUpdateFail(&memberNode->addr, false, message->transID, message->key, message->value + "-stabilization");
//            }
        }
    }
    return result;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(Message *message) {
    //do delete
    bool result = ht->deleteKey(message->key);
    //log the result
    if (result) {
        log->logDeleteSuccess(&memberNode->addr, false, message->transID, message->key);
    } else {
        log->logDeleteFail(&memberNode->addr, false, message->transID, message->key);
    }
    //construct a reply and send it
    Message reply = Message(message->transID, memberNode->addr, REPLY, result);
    if (sameAddress(&memberNode->addr, &message->fromAddr)) {
        // if it's local, call local function
        handleReply(&reply);
    } else {
        emulNet->ENsend(&memberNode->addr, &message->fromAddr, (char *) &reply, sizeof(Message));
    }
}

bool MP2Node::handleReply(Message *message) {
    Message *msgSent = sentMessages[message->transID];
    switch (msgSent->type) {
        case CREATE: {
            if (message->success) {
                successReplies[message->transID]++;
                if (successReplies[message->transID] > 1) {
                    log->logCreateSuccess(&msgSent->fromAddr, true, msgSent->transID, msgSent->key, msgSent->value);
                    successReplies[message->transID] = 0;
                }
            } else {
                failedReplies[message->transID]++;
                if (failedReplies[message->transID] > 1) {
                    log->logCreateFail(&msgSent->fromAddr, true, msgSent->transID, msgSent->key, msgSent->value);
                    failedReplies[message->transID] = 0;
                }
            }
            break;
        }
        case DELETE: {
            if (message->success) {
                successReplies[message->transID]++;
                if (successReplies[message->transID] > 1) {
                    log->logDeleteSuccess(&msgSent->fromAddr, true, msgSent->transID, msgSent->key);
                    successReplies[message->transID] = 0;
                }
            } else {
                failedReplies[message->transID]++;
                if (failedReplies[message->transID] > 1) {
                    log->logDeleteFail(&msgSent->fromAddr, true, msgSent->transID, msgSent->key);
                    failedReplies[message->transID] = 0;
                }
            }
            break;
        }
        case READ: {
            if (message->value.compare("") != 0) {
                successReplies[message->transID]++;
                if (successReplies[message->transID] > 1) {
                    log->logReadSuccess(&msgSent->fromAddr, true, msgSent->transID, msgSent->key, message->value);
                    successReplies[message->transID] = 0;
                    readcounter.erase(message->transID);
                }
            } else {
                failedReplies[message->transID]++;
                if (failedReplies[message->transID] > 1) {
                    log->logReadFail(&msgSent->fromAddr, true, msgSent->transID, msgSent->key);
                    failedReplies[message->transID] = 0;
                    readcounter.erase(message->transID);
                }
            }
            break;
        }
        case UPDATE: {
            if (message->success) {
                successReplies[message->transID]++;
                if (successReplies[message->transID] > 1) {
                    log->logUpdateSuccess(&msgSent->fromAddr, true, msgSent->transID, msgSent->key, msgSent->value);
                    successReplies[message->transID] = 0;
                    updatecounter.erase(message->transID);
                }
            } else {
                failedReplies[message->transID]++;
                if (failedReplies[message->transID] > 1) {
                    log->logUpdateFail(&msgSent->fromAddr, true, msgSent->transID, msgSent->key, msgSent->value);
                    failedReplies[message->transID] = 0;
                    updatecounter.erase(message->transID);
                }
            }
            break;
        }
        default:
            break;
    }
    return false;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        /*
         * Handle the message types here
         */
//        Message *msg_try = new Message(message_o);
        Message *message;
        message = (Message *) malloc(sizeof(Message));
        memcpy(message, data, sizeof(Message));
        switch (message->type) {
            case CREATE: {
                createKeyValue(message);
                break;
            }
            case UPDATE: {
                updateKeyValue(message);
                break;
            }
            case DELETE: {
                deletekey(message);
                break;
            }
            case READ: {
                readKey(message);
                break;
            }
            case REPLY: {
                handleReply(message);
                break;
            }
            case READREPLY: {
                stabilization(message);
                break;
            }
            default:
                break;
        }
    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
}

void MP2Node::checkReadResult() {
    if (readcounter.empty() && updatecounter.empty()) return;
    map<int, long>::iterator it;
    for (it = readcounter.begin(); it != readcounter.end(); it++) {
        if (par->getcurrtime() - it->second > 10) {
            Message *msg = sentMessages[it->first];
            log->logReadFail(&msg->fromAddr, true, msg->transID, msg->key);
            readcounter.erase(it->first);
            return;
        }
    }

    map<int, long>::iterator it2;
    for (it = updatecounter.begin(); it != updatecounter.end(); it++) {
        if (par->getcurrtime() - it->second > 10) {
            Message *msg = sentMessages[it->first];
            log->logUpdateFail(&msg->fromAddr, true, msg->transID, msg->key, msg->value);
            updatecounter.erase(it->first);
            return;
        }
    }
}


/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        checkReadResult();
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *) env, (void *) buff, size);
}


bool MP2Node::sameNode(Node a, Node b) {
    return (memcmp(a.getAddress(), b.getAddress(), sizeof(Address)) == 0 && a.getHashCode() == b.getHashCode());
}

bool MP2Node::sameRing(vector<Node> a, vector<Node> b) {
    if (a.size() != b.size()) return false;
    vector<Node>::iterator it_a;
    vector<Node>::iterator it_b;
    for (it_a = a.begin(), it_b = b.begin(); it_a != a.end(); it_a++, it_b++) {
        if (!sameNode(*it_a, *it_b)) {
            return false;
        }
    }
    return true;
}

bool MP2Node::sameAddress(Address *a, Address *b) {
    int id_a = *(int *) (a->addr);
    int id_b = *(int *) (b->addr);
//    short port_a = *(short *) (a->addr[4]);
//    short port_b = *(short *) (b->addr[4]);
    return (id_a == id_b);
}


