/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/**
 * FUNCTION NAME: convertMLEToAddr
 *
 * DESCRIPTION: converts MLE to Address
 */
Address convertMLEToAddr(MemberListEntry* mle) {

    Address a;
    memcpy(a.addr, &mle->id, sizeof(int));
    memcpy(&a.addr[4], &mle->port, sizeof(short));

    return a;
}

/**
 * FUNCTION NAME: convertAddrToMLE
 *
 * DESCRIPTION: converts Address to MLE
 */
MemberListEntry convertAddrToMLE(Address *addr) {

    MemberListEntry a;
    memcpy(&a.id, &addr->addr[0], sizeof(int));
    memcpy(&a.port, &addr->addr[4], sizeof(short));
    return a;
}


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
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
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
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
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
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
    if( initThisNode(&joinaddr) == -1 ) {
    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_this node failed. Exit.");
    #endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
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
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode, id, port);

    return 0;
}

/**
 * FUNCTION NAME: isIntroducer
 *
 * DESCRIPTION: Check is argument member is a Introducer
 */
bool MP1Node::isIntroducer(Member *node) {

    Address joinaddr = getJoinAddress();

    if ( 0 == memcmp( (char *)&(node->addr.addr), (char *)&(joinaddr.addr), sizeof(node->addr.addr)) ) {
        return true;
    } else {
        return false;
    }

}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	//joinaddr is address of coordinator
    MessageHdr *msg;
    #ifdef DEBUGLOG
    static char s[1024];
    #endif


    // I am the group booter (first process to join the group). Boot up the group
    if ( isIntroducer(memberNode) ) {

        #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
        #endif
        memberNode->inGroup = true;
    }
    // rest of nodes joining group by sending JOINREQ
    else {
        
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        //copy own address (&memberNode->addr.addr)
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        //copy own hearbeat (&memberNode->heartbeat)
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

        #ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
        #endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    // if node is marked as failed than exit nodeLoop
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    // if node is not in group yet than exit nodeLoop
    if( !memberNode->inGroup ) {
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
    while ( !memberNode->mp1q.empty() ) {
        //get first element from front of queue
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
        //remove from queue
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: processJoinRequest
 *
 * DESCRIPTION: process HEARTBEAT type of message
 *              processed by introducer, JOINREQ message from node is being processed and JOINREP message is send back to node
 */
void MP1Node::processJoinRequest(Address *addr, char *data, int size ) {
    cout << "Processing JOINREQ - START" << endl;

    MessageHdr* msg;
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = JOINREP;

    memcpy((char *)(msg+1), &memberNode->addr, sizeof(memberNode->addr));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr) + 1, &memberNode->heartbeat, sizeof(long));

    #ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Trying to send JOINREP to %s with heartbeat=%d", addr->getAddress().c_str(), memberNode->heartbeat);
    #endif
    
    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);
    free(msg);

    long *heartbeat = (long*)data;
   
    //update MemberList and sendGossip if there was an update
    if(updateMemberList(addr, *heartbeat)) 
      sendGossip(addr, *heartbeat);


    cout << "Processing JOINREQ - STOP" << endl;
}

/**
 * FUNCTION NAME: processJoinReply
 *
 * DESCRIPTION: process JOINREP type of message
 *              processed by node, who send JOINREQ
 */

void MP1Node::processJoinReply(Address *addr, char *data, int size ) {

    cout << "Processing JOINREP - START" << endl;

    //i got JOINREP response, so i am assigning myself to group
    memberNode->inGroup = 1;

    long *heartbeat = (long*)(data);

    #ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Got JOINREP from: %s with heartbeat=%d", addr->getAddress().c_str(), *heartbeat);
    #endif

    //update MemberList and sendGossip if there was an update
    if(updateMemberList(addr, *heartbeat)) 
      sendGossip(addr, *heartbeat);
    

    cout << "Processing JOINREP - START" << endl;
}


/**
 * FUNCTION NAME: processGossip
 *
 * DESCRIPTION: process HEARTBEAT type of message
 */

void MP1Node::processGossip(Address *addr, char *data, int size ) {

    long *heartbeat = (long*)data;

    //update MemberList and sendGossip if there was an update
    if(updateMemberList(addr, *heartbeat)) 
      sendGossip(addr, *heartbeat);

}

/**
 * FUNCTION NAME: sendGossip
 *
 * DESCRIPTION: sends gossip, HEARTBEAT type of message to random nodes from memberList
 *              srcNodeAddr - source node, who send gossip with
 *              dstNodeAddr - destination node, to which we send gossip
 */

void MP1Node::sendGossip(Address *srcNodeAddr, long heartbeat) {

    // prepare HEARTBEAT message
    MessageHdr *msg;

    size_t msgsize = sizeof(MessageHdr) + sizeof(srcNodeAddr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    msg->msgType = HEARTBEAT;
    memcpy((char *)(msg+1), srcNodeAddr->addr, sizeof(srcNodeAddr->addr));
    memcpy((char *)(msg+1) + sizeof(srcNodeAddr->addr) + 1, &heartbeat, sizeof(long));
    // ----------------------------------------------

    //iterate thru memberList
    for (vector<MemberListEntry>::iterator i = memberNode->memberList.begin(); i != memberNode->memberList.end(); i++) {

        //convert from MLE to Address
        Address dstNodeAddr = convertMLEToAddr(&(*i));
        if ( (dstNodeAddr == memberNode->addr) || (dstNodeAddr == *srcNodeAddr) ) {
            continue;
        }

        //gossip or not to gossip ?
        if ( rand() % 2 )
           emulNet->ENsend(&memberNode->addr, &dstNodeAddr, (char *)msg, msgsize);
    }

    free(msg);
}


/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    MessageHdr *msg = (MessageHdr *)data;
    Address *srcNodeAddr = (Address*)(msg+1);

    // msgSize = size of message without MessageHdr, Address and padding
    int msgSize = size - (sizeof(MessageHdr) + sizeof(Address) + 1);

    // pktData = pointer to Message Data (points after MessageHdr, Address and padding)
    char *pktData = data + (sizeof(MessageHdr) + sizeof(Address) + 1);

    //based on msgType take an action
    switch (msg->msgType) {

        case JOINREQ:
            processJoinRequest(srcNodeAddr, pktData, msgSize);
            break;

        case JOINREP:
            processJoinReply(srcNodeAddr, pktData, msgSize);
            break;

        case HEARTBEAT:
            processGossip(srcNodeAddr, pktData, msgSize);
            break;

        default:
            #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Unknown message type");
            #endif
            break;

    }



}


/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: function updates MemberList
 *              returns:
 *                  true  - if memberList was updated
 *                  false - if memberList was NOT updated
 */

bool MP1Node::updateMemberList(Address *addr, long heartbeat)  {

    vector<MemberListEntry>::iterator i;

    //for all entries in memberList for given node
    for (i = memberNode->memberList.begin(); i != memberNode->memberList.end(); i++) {

        //convert MLE to Address and compare to *addr
        if ( convertMLEToAddr(&(*i)) == *addr ) {

            //if address is the same and heartbeat is higher that in memberList - update memberList entry
            if (heartbeat > i->getheartbeat()) {

                i->setheartbeat(heartbeat);
                i->settimestamp(par->getcurrtime());

                return true;

            } else {

                return false;
            }
        }
    }

    //addr was not found in memberList - than push it to memberList vector

    //MemberListEntry(int id, short port, long heartbeat, long timestamp);
    MemberListEntry mle(*((int*)addr->addr), *((short*)&(addr->addr[4])), heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(mle);

    #ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, addr);
    #endif
    

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {


    for (vector<MemberListEntry>::iterator i = memberNode->memberList.begin(); i != memberNode->memberList.end(); i++) {

        // if (currenttime - timestamp) is > TFAIL (5 sec) than remove node from memberList
        if (par->getcurrtime() - i->timestamp > TFAIL) {

            //convert from MLE to Address
            Address addr = convertMLEToAddr(&(*i));
            
            #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Timeout");
            #endif
            
            vector<MemberListEntry>::iterator next_it = i;
            vector<MemberListEntry>::iterator next_next_it = i+1;
            for (next_it = i; next_next_it != memberNode->memberList.end(); next_it++, next_next_it++) {
                *next_it = *next_next_it;
            }
            memberNode->memberList.resize(memberNode->memberList.size()-1);
            i -= 1;
            
            #ifdef DEBUGLOG
            log->logNodeRemove(&memberNode->addr, &addr);
            #endif
            
        }
    }

    //increament own heartbeat and update memberList
    if(updateMemberList(&memberNode->addr, ++memberNode->heartbeat)) 
      sendGossip(&memberNode->addr, memberNode->heartbeat);

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
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */

void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {

	memberNode->memberList.clear();
    MemberListEntry mle = MemberListEntry(id, port);
    mle.settimestamp(par->getcurrtime());
    mle.setheartbeat(memberNode->heartbeat);
    memberNode->memberList.push_back(mle);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {

    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]);
}
