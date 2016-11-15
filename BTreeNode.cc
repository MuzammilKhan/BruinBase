#include "BTreeNode.h"
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <cmath>

using namespace std;

BTLeafNode::BTLeafNode(){//(PageId pid){
	std::fill(buffer, buffer+ PageFile::PAGE_SIZE, -1); //Initialize buffer to some value
														//Do we want to use -1 or 0?
	//m_pid = pid;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid, buffer); }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid, buffer); }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int intSize = sizeof(int); 
	int pairSize = intSize + sizeof(RecordId);
	int keyCount = 0;
	char* bufPtr = buffer;
	int i = 0;
	int key;

	for(; i < PageFile::PAGE_SIZE ; i += pairSize, bufPtr += pairSize ) {
		memcpy(&key, bufPtr, intSize);
		if(key == -1) break;  //If hit an element in the buffer we didn't set, stop counting. NOTE: change compare to -1 or 0 based off initialization
		keyCount++;
	}
	return keyCount; }

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	int pageIdSize = sizeof(PageId);
	int intSize = sizeof(int);
	PageId nextPtr;
	char* bufPtr = buffer;
	memcpy(&nextPtr, bufPtr + PageFile::PAGE_SIZE - pageIdSize, pageIdSize);

	int pairSize = sizeof(int) + sizeof(RecordId);
	if(getKeyCount() + 1 > (PageFile::PAGE_SIZE - pageIdSize)/pairSize ) { 
		return RC_NODE_FULL;
	}

	//assuming keys are in ascending order, makes checking with -1 easy
	int i = 0;
	int keyTmp;
	for(; i < PageFile::PAGE_SIZE - pageIdSize; i += pairSize, bufPtr += pairSize) {	
		memcpy(&keyTmp, bufPtr, intSize);
		if(keyTmp == -1 || !(key > keyTmp)) {break;} //stop when at end of keys or key we want to insert is greater than key in buffer

	}


	//Copy the the buffer into the tmp buffer until the point where we stopped in the loop above
	// Insert key,value pair and then the rest of the buffer into the tmp buffer
	//Now copy the whole tmp buffer back and overwrite the buffer
	int keyCount = getKeyCount();
	char* tmpBuf = (char*) malloc(PageFile::PAGE_SIZE);
	std::fill(tmpBuf, tmpBuf+ PageFile::PAGE_SIZE, -1);
	memcpy(tmpBuf, buffer, i);
	memcpy(tmpBuf + i, &key, intSize);
	memcpy(tmpBuf + i + intSize, &rid, sizeof(RecordId));
	memcpy(tmpBuf + i + pairSize, buffer + i, keyCount * pairSize - i);
	memcpy(tmpBuf + PageFile::PAGE_SIZE - pageIdSize, &nextPtr, pageIdSize);
	memcpy(buffer, tmpBuf, PageFile::PAGE_SIZE);
	free(tmpBuf);

	return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	int intSize = sizeof(int); 
	int pairSize = intSize + sizeof(RecordId);
	int pageIdSize = sizeof(PageId);


	if(!(getKeyCount() + 1 > (PageFile::PAGE_SIZE - pageIdSize)/pairSize )) { 
		return RC_INVALID_FILE_FORMAT; //trying to split when there is no overflow results in bad format
	}
	if(sibling.getKeyCount() != 0) {
		return RC_INVALID_ATTRIBUTE; //sibling must be empty, if it isnt this is invalid
	}	

	//SPLITTING CODE

	PageId nextPtr;
	char* bufPtr = buffer;
	memcpy(&nextPtr, bufPtr + PageFile::PAGE_SIZE - pageIdSize, pageIdSize);	

	int keepKeysCount = (int) ceil(((float) getKeyCount() + 1)/2); //number of keys to keep in this node
	int splitIndex = keepKeysCount*pairSize; //index to split at

	//copy everything past the split index to the sibling
	memcpy(sibling.buffer, buffer + splitIndex, PageFile::PAGE_SIZE - pageIdSize - splitIndex);
	sibling.setNextNodePtr(nextPtr); 
	//setNextNodePtr(sibling.m_pid);

	//clear pairs that we copied over to sibling from this node
	std::fill(buffer + splitIndex, buffer + PageFile::PAGE_SIZE - pageIdSize, -1);

	//INSERTION CODE
	memcpy(&siblingKey, sibling.buffer, intSize); //first key in sibling
	if(key >= siblingKey) { //figure out whether to put key here or in sibling
		sibling.insert(key, rid);
	} else {
		insert(key,rid);
	}
	RecordId siblingRid; //We need to intialize the sid and pid of sibling's rid
	siblingRid.sid = -1;
	siblingRid.pid = -1;
	memcpy(&siblingRid, sibling.buffer + intSize, sizeof(RecordId));

	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int intSize = sizeof(int);
	int pairSize = sizeof(int) + sizeof(RecordId);
	char* bufPtr = buffer;
	int i = 0;
	int condition = getKeyCount()  * pairSize;
	int key;

	for(; i < condition; i += pairSize , bufPtr += pairSize) {
		memcpy(&key, bufPtr, intSize);
		if( key == searchKey) {
			eid = i/pairSize;
			return 0;
		} else if( key == -1) {
		        break;
		} else if (key < searchKey) {
		        eid = i/pairSize;
		} else {}
	}

	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	if (eid < 0 || eid > getKeyCount() ) { //CHECK: should this be >= or > ??
		return RC_NO_SUCH_RECORD;
	}
	int intSize = sizeof(int);
	int pairSize = sizeof(int) + sizeof(RecordId);
	char* bufPtr = buffer;
	int pairLocation = eid * pairSize;

	memcpy(&key, bufPtr + pairLocation, intSize);
	memcpy(&rid, bufPtr + pairLocation + intSize, sizeof(rid));

	return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId pid;
	char* bufPtr = buffer;
	memcpy(&pid, bufPtr + PageFile::PAGE_SIZE - sizeof(PageId), sizeof(PageId));
	return pid;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	if(pid < 0){ return RC_INVALID_PID;}

	char* bufPtr = buffer;
	memcpy(bufPtr + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));
	return 0; 
}

void BTLeafNode::print()
{
	//This is the size in bytes of an entry pair
	int pairSize = sizeof(RecordId) + sizeof(int);
	
	char* temp = buffer;
	cout << "L ";

	for(int i=0; i<getKeyCount()*pairSize; i+=pairSize)
	{
		int insideKey;
		memcpy(&insideKey, temp, sizeof(int)); //Save the current key inside buffer as insideKey
		
		cout << insideKey << " ";
		
		temp += pairSize; 
	}
	
	//cout << "" << endl;
}

BTNonLeafNode::BTNonLeafNode(){ //(PageId pid){
	std::fill(buffer, buffer+ PageFile::PAGE_SIZE, -1); //Initialize buffer to some value
														//Do we want to use -1 or 0?
	//m_pid = pid;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid, buffer); }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid,buffer); }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount(){

	// representation:
	// [pid | key | pid | ... | key | pid]

	int intSize = sizeof(int); 
	int pageIdSize = sizeof(PageId);
	int pairSize = intSize + pageIdSize;
	int keyCount = 0;
	char* bufPtr = buffer + pageIdSize;
	int i = pageIdSize;
	int key;

	for(; i < PageFile::PAGE_SIZE ; i += pairSize, 	bufPtr += pairSize) {
		memcpy(&key, bufPtr, intSize);
		if(key == -1) break;  //If hit an element in the buffer we didn't set, stop counting. NOTE: change compare to -1 or 0 based off initialization
		keyCount++;
	}
	return keyCount;  
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	// representation:
	// [pid | key | pid | ... | key | pid]

	int pageIdSize = sizeof(PageId);
	int intSize = sizeof(int);
	PageId nextPtr;
	char* bufPtr = buffer;
	memcpy(&nextPtr, bufPtr + PageFile::PAGE_SIZE - pageIdSize, pageIdSize);

	int pairSize = intSize + pageIdSize;
	int keyCount = getKeyCount();
	if(keyCount + 1 > (PageFile::PAGE_SIZE - pageIdSize)/pairSize ) { 
		return RC_NODE_FULL;
	}

	//assuming keys are in ascending order, makes checking with -1 easy
	int i = pageIdSize;
	int keyTmp;
	for(; i < PageFile::PAGE_SIZE - pageIdSize ; i += pairSize, bufPtr += pairSize) {	
		memcpy(&keyTmp, bufPtr, intSize);
		if(keyTmp == -1 || (key > keyTmp)) {break;} //stop when at end of keys or key we want to insert is greater than key in buffer

	}


	//Copy the the buffer into the tmp buffer until the point where we stopped in the loop above
	// Insert key,value pair and then the rest of the buffer into the tmp buffer
	//Now copy the whole tmp buffer back and overwrite the buffer
	char* tmpBuf = (char*) malloc(PageFile::PAGE_SIZE);
	std::fill(tmpBuf, tmpBuf+ PageFile::PAGE_SIZE, -1);
	memcpy(tmpBuf, buffer, i);
	memcpy(tmpBuf + i, &key, intSize);
	memcpy(tmpBuf + i + intSize, &pid, pageIdSize);
	memcpy(tmpBuf + i + pairSize, buffer + i, keyCount * pairSize - i);
	memcpy(tmpBuf + PageFile::PAGE_SIZE - pageIdSize, &nextPtr, pageIdSize);
	memcpy(buffer, tmpBuf, PageFile::PAGE_SIZE);
	free(tmpBuf);

	return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	int intSize = sizeof(int); 
	int pageIdSize = sizeof(PageId);
	int pairSize = intSize + pageIdSize;


	if(!(getKeyCount() + 1 > (PageFile::PAGE_SIZE - pageIdSize)/pairSize )) { 
		return RC_INVALID_FILE_FORMAT; //trying to split when there is no overflow results in bad format
	}
	if(sibling.getKeyCount() != 0) {
		return RC_INVALID_ATTRIBUTE; //sibling must be empty, if it isnt this is invalid
	}	

	//SPLITTING CODE

	PageId nextPtr;
	char* bufPtr = buffer;
	memcpy(&nextPtr, bufPtr + PageFile::PAGE_SIZE - pageIdSize, pageIdSize);	

	int keepKeysCount = (int) ceil(((float) getKeyCount() + 1)/2); //number of keys to keep in this node
	int splitIndex = keepKeysCount*pairSize + pageIdSize; //index to split at

	int last_leftkey;
	int first_rightkey;

	memcpy(&last_leftkey, bufPtr + splitIndex - pairSize, intSize);
	memcpy(&first_rightkey, bufPtr + splitIndex, intSize);

	if (key < last_leftkey) { // last_leftkey is median key
		memcpy(sibling.buffer, buffer + splitIndex - pageIdSize, pageIdSize);
		memcpy(sibling.buffer + pageIdSize, buffer + splitIndex, PageFile::PAGE_SIZE - pageIdSize - splitIndex);
		memcpy(&midKey, buffer + splitIndex - pairSize, intSize);

		std::fill(buffer + splitIndex - pairSize, buffer + PageFile::PAGE_SIZE, -1); // remove copied over elements

		insert(key, pid);

	} else if (key > first_rightkey) { // first_rightkey is median key
		memcpy(sibling.buffer, buffer + splitIndex + intSize, pageIdSize); // pid
		memcpy(sibling.buffer + intSize, buffer + splitIndex + pairSize, PageFile::PAGE_SIZE - splitIndex - pairSize - pageIdSize);
		memcpy(&midKey, buffer + splitIndex, intSize); // save mid key

		std::fill(buffer + splitIndex, buffer + PageFile::PAGE_SIZE, -1);

		sibling.insert(key, pid);

	} else { // key is median key
		memcpy(sibling.buffer + pageIdSize, buffer + splitIndex, PageFile::PAGE_SIZE - splitIndex - pageIdSize);
		memcpy(sibling.buffer, &pid, pageIdSize);
		midKey = key;
		std::fill(buffer + splitIndex, buffer + PageFile::PAGE_SIZE, -1);
	}

	return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	// representation:
	// [pid | key | pid | ... | key | pid]

	int pageIdSize = sizeof(PageId);
	int intSize = sizeof(int);
	int pairSize = intSize + pageIdSize;

	char* bufPtr = buffer + pageIdSize; // start at first key
	int i = pageIdSize;
	for (; i < PageFile::PAGE_SIZE - pageIdSize; i += pairSize, bufPtr += pairSize) {
		int keyTmp;
		memcpy(&keyTmp, bufPtr, intSize);

		// there are no more keys, jump to next node
		if (keyTmp == -1) {
			break;
		}

		// return pid to left
		if (keyTmp > searchKey) {
			memcpy(&pid, bufPtr - pageIdSize, pageIdSize);
			return 0;
		}
	}

	// copy pointer to next node
	memcpy(&pid, buffer + PageFile::PAGE_SIZE - pageIdSize, pageIdSize);
	return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	std::fill(buffer, buffer + PageFile::PAGE_SIZE, -1); // initialize buffer contents

	char* bufptr = buffer;
	int pageIdSize = sizeof(PageId);
	int intSize = sizeof(int);

	memcpy(bufptr, &pid1, pageIdSize);
	memcpy(bufptr + pageIdSize, &key, intSize);
	memcpy(bufptr + pageIdSize + intSize, &pid2, pageIdSize);

	return 0;
}

void BTNonLeafNode::print()
{
	//This is the size in bytes of an entry pair
	int pairSize = sizeof(PageId) + sizeof(int);
	
	//Skip the first 8 offset bytes, since there's no key there
	char* temp = buffer + sizeof(PageId);

	cout << "N ";
	for(int i=0; i<getKeyCount()*pairSize; i+=pairSize)
	{
		int insideKey;
		memcpy(&insideKey, temp, sizeof(int)); //Save the current key inside buffer as insideKey

		cout << insideKey << " ";
		
		temp += pairSize; //Jump temp over to the next key
	}
	
	//cout << "" << endl;	
}
