#include "BTreeNode.h"

using namespace std;

BTLeafNode::BTLeafNode(){
	std::fill(buffer, buffer+ PageFile::PAGE_SIZE, -1); //Initialize buffer to some value
														//Do we want to use -1 or 0?
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
	if(getKeyCount() + 1 > (bufPtr + PageFile::PAGE_SIZE - pageIdSize)/pageIdSize ) { 
		return RC_NODE_FULL;
	}

	//assuming keys are in decending order, makes checking with -1 easy
	int i = 0;
	int keyTmp;
	for(; i < PageFile::PAGE_SIZE ; i += pairSize, bufPtr += pairSize) {	
		memcpy(&keyTmp, bufPtr, intSize);
		if(key == -1 || (key < keyTmp)) {break;} //stop when at end of keys or key we want to insert is smaller than key in buffer

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
{ return 0; }

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
		if( key >= searchKey) {
			eid = i/pairSize;
			return 0;
		}
	}
	eid = getKeyCount();
	return 0; 
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
	memcpy(&rid, bufPtr + pairSize + intSize; sizeof(rid));

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

	char* bufPtr;
	memcpy(bufPtr + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));
	return 0; 
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
	int intSize = sizeof(int); 
	int pairSize = intSize + sizeof(RecordId);
	int keyCount = 0;
	char* bufPtr = buffer + pairSize;
	int i = pairSize;
	int key;

	for(; i < PageFile::PAGE_SIZE ; i += pairSize, 	bufPtr += pairSize) {
		memcpy(&key, bufPtr, intSize);
		if(key == -1) break;  //If hit an element in the buffer we didn't set, stop counting. NOTE: change compare to -1 or 0 based off initialization
		keyCount++;
	}
	return keyCount;  }


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ return 0; }

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
{ return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ return 0; }

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
