/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;

	//ringSize stores size of ring
	//here we save initial ring size
	this->ringSize = par->EN_GPSZ;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}




/*
 * This function handles stale node.
 */
void MP2Node::handleStaleNodes() {

	size_t pos, start;
	string key, val;
	int timestamp;
	vector<string> tuple;

	//key = tranID; value= operationtype::timestamp::successcount::failurecount::key::value

	//iterate over entries in statusHashTable
	for (auto i = statusHashTable->hashTable.begin(); i != statusHashTable->hashTable.end(); i++) {

		key = i->first;
		val = i->second;
		pos = val.find("::");

		start = 0;

		while (pos != string::npos) {
			string field = val.substr(start, pos-start);
			tuple.push_back(field);
			start = pos + 2;
			pos = val.find("::", start);
		}

		tuple.push_back(val.substr(start));
		timestamp = stoi(tuple.at(1));

		if( (par->getcurrtime() - timestamp) > 10) {
			
			if(tuple.at(0) == "READ") {
				log->logReadFail(&memberNode->addr, true, stoi(key), val);
				statusHashTable->deleteKey(key);
			}

			if(tuple.at(0) == "UPDATE") {
				log->logUpdateFail(&memberNode->addr, true, stoi(key), val, tuple.at(5));
				statusHashTable->deleteKey(key);
			}
		}

	}
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

	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	
	//if ring size changed mark it in change boolean
	if(this->ringSize != curMemList.size()) {
		this->ringSize = curMemList.size();
		change = true;
	}


	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;



	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(ht->currentSize() > 0 && change){
		stabilizationProtocol();

	}

	/*
	 * Step 4: Handle stale nodes
	 */

	handleStaleNodes();

	
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
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
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
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {

	// increment g_transID (stored in common.h)
	g_transID++;

	// find replicas for a 'key' and store in keyReplicas
	vector<Node> keyReplicas = findNodes(key);

	//iterate over replicas and send message to them
	for(vector<Node>::iterator i = keyReplicas.begin(); i != keyReplicas.end(); i++){
		
		//construct a message for replica
		//transID::fromAddr::CREATE::key::value::ReplicaType
		//                    0          1          2
		//enum ReplicaType {PRIMARY, SECONDARY, TERTIARY};
		Message *mesg = new Message(g_transID, 
																memberNode->addr, 
																MessageType::CREATE, 
																key, 
																value, 
																ReplicaType(distance(keyReplicas.begin(), i))
																);
		
		//send message to replica
		emulNet->ENsend(&(memberNode->addr), (*(i)).getAddress(), mesg->toString());
	}

	//insert to status HashTable, example
	//key = 104
	//value = CREATE::275::0::0::2CTSr::newValue

	//HashTbale -> bool create(string key, string value);
	statusHashTable->create(to_string(g_transID),
													"CREATE::" + to_string(par->getcurrtime())+ "::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){

	// increment g_transID (stored in common.h)
	g_transID++;

	// find replicas for a 'key' ans store in keyReplicas
	vector<Node> keyReplicas = findNodes(key);


	//iterate over replicas and send message to them
	for(vector<Node>::iterator i = keyReplicas.begin(); i != keyReplicas.end(); i++){
		
		//construct a message for replica
		//transID::fromAddr::CREATE::key::value::ReplicaType
		//                    0          1          2
		//enum ReplicaType {PRIMARY, SECONDARY, TERTIARY};
		Message *mesg = new Message(g_transID, 
																memberNode->addr, 
																MessageType::READ, 
																key
																);
		
		//send message to replica
		emulNet->ENsend(&(memberNode->addr), (*(i)).getAddress(), mesg->toString());
	}

	//insert to status HashTable, example
	//key = 104
	//value = CREATE::275::0::0::2CTSr::newValue

	//HashTbale -> bool create(string key, string value);
	statusHashTable->create(to_string(g_transID),
													"READ::" + to_string(par->getcurrtime())+ "::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + "");



}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){

	// increment g_transID (stored in common.h)
	g_transID++;

	// find replicas for a 'key' and store in keyReplicas
	vector<Node> keyReplicas = findNodes(key);

	//iterate over replicas and send message to them
	for(vector<Node>::iterator i = keyReplicas.begin(); i != keyReplicas.end(); i++){
		
		//construct a message for replica
		//transID::fromAddr::CREATE::key::value::ReplicaType
		//                    0          1          2
		//enum ReplicaType {PRIMARY, SECONDARY, TERTIARY};
		Message *mesg = new Message(g_transID, 
																memberNode->addr, 
																MessageType::UPDATE, 
																key, 
																value, 
																ReplicaType(distance(keyReplicas.begin(), i))
																);
		
		//send message to replica
		emulNet->ENsend(&(memberNode->addr), (*(i)).getAddress(), mesg->toString());
	}

	//insert to status HashTable, example
	//key = 104
	//value = CREATE::275::0::0::2CTSr::newValue

	//HashTbale -> bool create(string key, string value);
	statusHashTable->create(to_string(g_transID),
													"UPDATE::" + to_string(par->getcurrtime())+ "::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + value);
}
/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){

	// increment g_transID (stored in common.h)
	g_transID++;

	// find replicas for a 'key' ans store in keyReplicas
	vector<Node> keyReplicas = findNodes(key);


	//iterate over replicas and send message to them
	for(vector<Node>::iterator i = keyReplicas.begin(); i != keyReplicas.end(); i++){
		
		//construct a message for replica
		//transID::fromAddr::CREATE::key::value::ReplicaType
		//                    0          1          2
		//enum ReplicaType {PRIMARY, SECONDARY, TERTIARY};
		Message *mesg = new Message(g_transID, 
																memberNode->addr, 
																MessageType::DELETE, 
																key
																);
		
		//send message to replica
		emulNet->ENsend(&(memberNode->addr), (*(i)).getAddress(), mesg->toString());
	}

	//insert to status HashTable, example
	//key = 104
	//value = CREATE::275::0::0::2CTSr::newValue

	//HashTbale -> bool create(string key, string value);
	statusHashTable->create(to_string(g_transID),
													"DELETE::" + to_string(par->getcurrtime())+ "::" + to_string(0) + "::" + to_string(0) + "::" + key + "::" + "");
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, Address coodAddr) {

	//construct Entry object
	Entry *en = new Entry(value, par->getcurrtime(), replica);
	
	// Insert key, value, replicaType into local hash table
	bool result = ht->create(key, en->convertToString());

	//transID = -999 for create operations during stabilization protocol
	if(transID != -999) {
		
		//if result is true log that create finished with success
		if(result) {
			log->logCreateSuccess(&(memberNode->addr), false, transID, key, value);
		}
		//if result is false log that create finished with failure
		else {
			log->logCreateFail(&(memberNode->addr), false, transID, key, value);
		}

		//send message with result
		Message *msg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
		emulNet->ENsend(&(memberNode->addr), &coodAddr, msg->toString());
	}

	//return true or false based on success or failure
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
string MP2Node::readKey(string key, int transID, Address coodAddr) {
	
	// Read key from local hash table and return value
	string result = ht->read(key);

	//if result if empty log that read failed
	if(result.empty()) {
		log->logReadFail(&(memberNode->addr), false, transID, key);
	}
	//if result if not empty log that read was success
	else {
		Entry *en = new Entry(result);
		log->logReadSuccess(&(memberNode->addr), false, transID, key, en->value);
	}

	//send message with result
	Message *msg = new Message(transID, memberNode->addr, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, msg->toString());

	//return result containing value for given key
	return result;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID, Address coodAddr) {
	
	//contruct Entry object
	Entry *en = new Entry(value, par->getcurrtime(), replica);
	
	// Update key in local hash table
	bool result = ht->update(key, en->convertToString());
	
	//if result is true log that update finished with success
	if(result) {
		log->logUpdateSuccess(&(memberNode->addr), false, transID, key, value);
	}
	//if result is false log that update finished with failure
	else {
		log->logUpdateFail(&(memberNode->addr), false, transID, key, value);
	}

	//send message with result
	Message *msg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, msg->toString());
	
	//return true or false
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
bool MP2Node::deletekey(string key, int transID, Address coodAddr) {
	
	// Delete the key from the local hash table
	bool result = ht->deleteKey(key);
	
	//if result is true log that delete finished with success
	if(result) {
		log->logDeleteSuccess(&(memberNode->addr), false, transID, key);
	}
	//if result is false log that delete finished with failure
	else {
		log->logDeleteFail(&(memberNode->addr), false, transID, key);
	}

	//send message with result
	Message *msg = new Message(transID, memberNode->addr, MessageType::REPLY, result);
	emulNet->ENsend(&(memberNode->addr), &coodAddr, msg->toString());

	return result;
}

/**
 * FUNCTION NAME: processReplyMessages
 *
 * DESCRIPTION: process REPLY message, sent after following operations CREATE, UPDATE, DELETE 
 * 				This function does the following:
 * 				1) if coordinator got QUORUM number of successfull replies log a successful message
 * 				2) else log a failure message
 * 
 */
void MP2Node::processReplyMessages(Message *msg) {

	/*
	Message is in following format:
	new Message(transID, memberNode->addr, MessageType::REPLY, result);
	example:
	message = 59::8:0::4::1
	59  - transID
	8:0 - addr
	4   - MessageType::REPLY
	1   - status (success) 
	*/

	//cout << "message = " << msg->toString() << endl;
	size_t pos, start;
	int successCount = 0, failureCount = 0;
	vector<string> tuple;

	// fetch all transation details from status Hash Table for given transaction id (transID) from message
	string val = statusHashTable->read(to_string(msg->transID));

	//if retrived value is not empty meaning we were able to fetch from status Hash Table
	if( !val.empty() ) {

		pos = val.find("::");
		start = 0;

		while (pos != string::npos) {

			string field = val.substr(start, pos-start);
			tuple.push_back(field);
			start = pos + 2;
			pos = val.find("::", start);

		}

		tuple.push_back(val.substr(start));

		successCount = stoi(tuple.at(2));
		failureCount = stoi(tuple.at(3));

		//if success increament success counter
		if(msg->success) {
			successCount++;
		}
		//if failure increament failure counter
		else {
			failureCount++;
		}
		
		//update status Hash Table
		statusHashTable->update(to_string(msg->transID), 
														tuple.at(0) + "::" + tuple.at(1) + "::" + to_string(successCount) + "::" + to_string(failureCount)+ "::" + tuple.at(4) + "::" + tuple.at(5));

		//message type CREATE
		if(tuple.at(0) == "CREATE") {
			
			if(successCount == 2) {

				//log that create was success
				log->logCreateSuccess(&(memberNode->addr), true, msg->transID, tuple.at(4), tuple.at(5));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}
			
			else if(failureCount == 2) {

				//log that create was failure
				log->logCreateFail(&(memberNode->addr), true, msg->transID, tuple.at(4), tuple.at(5));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}

		}

		//message type DELETE
		else if(tuple.at(0) == "DELETE"){
			
			if(successCount == 2) {

				//log that delete was success
				log->logDeleteSuccess(&(memberNode->addr), true, msg->transID, tuple.at(4));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}
			
			else if(failureCount == 2) {

				//log that delete was failure
				log->logDeleteFail(&(memberNode->addr), true, msg->transID, tuple.at(4));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}

		}

		//message type UPDATE
		else if(tuple.at(0) == "UPDATE"){
			
			if(successCount == 2) {

				//log that update was success
				log->logUpdateSuccess(&(memberNode->addr), true, msg->transID, tuple.at(4), tuple.at(5));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}
			
			else if(failureCount == 2) {

				//log that create was failure
				log->logUpdateFail(&(memberNode->addr), true, msg->transID, tuple.at(4), tuple.at(5));

				//remove key from status Hash Table for transID
				statusHashTable->deleteKey(to_string(msg->transID));
			}

		}
	}
}

/**
 * FUNCTION NAME: processReadReplyMessages
 *
 * DESCRIPTION: process READREPLY message, sent after following operations READ
 * 				This function does the following:
 * 				1) if coordinator got QUORUM number of successfull replies log a successful message
 * 				2) else log a failure message
 */
void MP2Node::processReadReplyMessages(Message *msg) {

	size_t pos, start;
	int successCount = 0, failureCount = 0;
	vector<string> tuple;

	// fetch all transation details from status Hash Table for given transaction id (transID) from message
	string val = statusHashTable->read(to_string(msg->transID));

	if( !val.empty() ) {
		
		pos = val.find("::");
		start = 0;
		
		while (pos != string::npos) {
			string field = val.substr(start, pos-start);
			tuple.push_back(field);
			start = pos + 2;
			pos = val.find("::", start);
		}

		tuple.push_back(val.substr(start));
		
		successCount = stoi(tuple.at(2));
		failureCount = stoi(tuple.at(3));


		if( !((msg->value).empty()) ) {

			successCount++;

			Entry *en = new Entry(msg->value);
			statusHashTable->update(to_string(msg->transID), 
															tuple.at(0) + "::" + tuple.at(1) + "::" + to_string(successCount) + "::" + to_string(failureCount)+ "::" + tuple.at(4) + "::" + en->value);
		}		
		else{

			failureCount++;

			statusHashTable->update(to_string(msg->transID), 
															tuple.at(0) + "::" + tuple.at(1) + "::" + to_string(successCount) + "::" + to_string(failureCount)+ "::" + tuple.at(4) + "::" + tuple.at(5));
		}

		//Logging success and fail messages
		if( successCount == 2 ) {
			Entry *en = new Entry(msg->value);

			// log that read was successfull
			log->logReadSuccess(&(memberNode->addr), true, msg->transID, tuple.at(4), en->value);
			statusHashTable->deleteKey(to_string(msg->transID));
		}
		else if( failureCount == 2 ) {

			// log that read failed
			log->logReadFail(&(memberNode->addr), true, msg->transID, tuple.at(4));
			statusHashTable->deleteKey(to_string(msg->transID));
		}

	}
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

	char *data;
	int size;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

 		Message *msg = new Message(message);

 		//cout << "message = " << msg->toString() << endl;

		// CREATE, DELETE, READ, UPDATE, REPLY, READREPLY
		switch(msg->type)
		{
			case CREATE:
				createKeyValue(msg->key, msg->value, msg->replica, msg->transID, msg->fromAddr);
				break;

			case DELETE:
				deletekey(msg->key, msg->transID, msg->fromAddr);
				break;

			case READ:
				readKey(msg->key, msg->transID, msg->fromAddr);
				break;

			case UPDATE:
				updateKeyValue(msg->key, msg-> value, msg->replica, msg->transID, msg->fromAddr);
				break;

			case REPLY:
				processReplyMessages(msg);
				break;

			case READREPLY:
				processReadReplyMessages(msg);
				break;

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
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
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
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
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
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {

	//Step 1: loop over entire ring and find neighbours
	for ( int i = 0; i < ring.size(); i++ ) {

		Node node = ring.at(i);
		
		// if we found in loop that node's address is 'our' address than update neighbours
		if(( node.nodeAddress ) == (memberNode->addr)) {

			//hasMyReplicas and haveReplicasOf are private attributes of Class MP2Node
			
			//update hasMyReplicas
			hasMyReplicas.clear();
			//next node (i+1) has my replica
			hasMyReplicas.push_back( ring.at((i + 1) % ring.size()) );
			//next next node (i+2) has my replica
			hasMyReplicas.push_back( ring.at((i + 2) % ring.size()) );
			
			//update haveReplicasasOf
			haveReplicasOf.clear();
			//i have replica of prev node (i-1)
			haveReplicasOf.push_back( ring.at((i - 1) % ring.size()) );
			//i have replica of prev prev node (i-2)
			haveReplicasOf.push_back( ring.at((i - 2) % ring.size()) );
		}

	}

	//Step 2: iterate over hash table and make sure there are 3 replicas
	string key,value;
	Message *msg;
	for(map<string,string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
		
		key = it->first;
		value = it->second;

		Entry *en = new Entry(value);

		switch(en->replica) {

			case PRIMARY:

				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::SECONDARY);
				emulNet->ENsend(&(memberNode->addr), hasMyReplicas.at(0).getAddress(), msg->toString());
				
				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::TERTIARY);
				emulNet->ENsend(&(memberNode->addr), hasMyReplicas.at(1).getAddress(), msg->toString());
				break;

			case SECONDARY:

				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::TERTIARY);
				emulNet->ENsend(&(memberNode->addr), hasMyReplicas.at(0).getAddress(), msg->toString());

				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::PRIMARY);
				emulNet->ENsend(&(memberNode->addr), haveReplicasOf.at(0).getAddress(), msg->toString());
				break;

			case TERTIARY:

				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::PRIMARY);
				emulNet->ENsend(&(memberNode->addr), haveReplicasOf.at(1).getAddress(), msg->toString());

				// use -999 value of transaction id to distinguish it from other CREATE messages
				msg = new Message(-999, memberNode->addr, MessageType::CREATE, key, en->value, ReplicaType::SECONDARY);
				emulNet->ENsend(&(memberNode->addr), haveReplicasOf.at(0).getAddress(), msg->toString());
				break;
		}
	}
}
