/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstring>
#include <cstdlib>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0;
    std::fill(index_buffer, index_buffer + PageFile::PAGE_SIZE, -1); // empty out buffer
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC rc = pf.open(indexname, mode);
	if(rc < 0) {
		return rc;
	}

	// create index file if it doesn't exist and update disk entry with buffer info
	// if index file doesn't exist, this is the first time this tree is being used
	// ^possibly reinitalize rootPid and treeHeight to be sure of values?
	if(pf.endPid() <= 0) {
		rc = pf.write(0, index_buffer);
		if (rc < 0) {
			return rc;
		}
	} else {
		rc = pf.read(0, index_buffer);
		if (rc < 0) {
			return rc;
		}

		int t_pid, t_height;
		memcpy(&t_pid, index_buffer, intSize);
		memcpy(&t_height, index_buffer + intSize, intSize);

		// ensure stored values are valid before setting member variables
		// note: we use pid = 0 for the index file by default so we cannot store the tree there
		if (t_pid > 0 && t_height >= 0) {
			rootPid = t_pid;
			treeHeight = t_height;
		}
	}

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	memcpy(index_buffer, &rootPid, intSize);
	memcpy(index_buffer + intSize, &treeHeight, intSize);
    
    // store rootpid and treeheight before closing file
    RC rc = pf.write(0, index_buffer);
    if (rc < 0) {
    	return rc;
    } else {
    	return pf.close();
    }
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	//search to determine what bucket to put record in

	//try to insert, if successful done.

	//else if node was full insert and split, if this fails b/c parent is full split it too
	//add middle key to parent node and repeat until we find a parent we dont have to split.
	//if root splits create a new root with one key and two pointers (new value gets pushed 
	//to new root and removed from original node)
    return 0;
}

//recursive helper function for locate
RC BTreeIndex::search_tree( int searchKey, IndexCursor& cursor, int currHeight, PageId& nextPid) {

	//check for valid search key
	if(searchKey < 0) {
		return RC_INVALID_ATTRIBUTE;
	}

	RC rc;
	//base case at leaf
	if(currHeight == treeHeight) {
		BTLeafNode leaf;
		if( (rc = leaf.read(nextPid, pf)) < 0) {
			return rc;
		}

		if( (rc = leaf.locate(searchKey, cursor.eid)) < 0) {
			return rc;
		}

		cursor.pid = nextPid;
		return 0;
	}

	//recursive step check for errors and go down to leaf
	BTNonLeafNode nonLeaf;
	if( (rc = nonLeaf.read(nextPid, pf)) < 0) {
		return rc;
	}	

	if( (rc = nonLeaf.locateChildPtr(searchKey, nextPid)) < 0) {
		return rc;
	}

	return search_tree(searchKey, cursor, currHeight - 1, nextPid);

}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	return search_tree(searchKey, cursor, treeHeight, rootPid); 
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	RC rc;
	BTLeafNode leaf;

	if( (rc = leaf.read(cursor.pid, pf)) < 0) { //Do we have any error checking in read though?
		return rc;
	}

	rc = leaf.readEntry(cursor.eid, key, rid);
	if( rc == 0) {
		cursor.eid++;
		return 0;
	} else if (rc == RC_NO_SUCH_RECORD) { 
		//go to beginning of next node
		cursor.pid = leaf.getNextNodePtr();
		cursor.eid = 0;
		if(cursor.pid == -1) { //check if at end of tree
			return RC_END_OF_TREE;
		}
	} else {
		return RC_INVALID_CURSOR;
	}

    return 0;
}
