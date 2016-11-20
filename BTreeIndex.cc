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
#include <iostream>

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
	  cout << "Index file doesn't exist, creating it now..." << endl;
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

//recursive helper for insert
RC BTreeIndex::rec_insert(int key, const RecordId& rid, int currHeight, PageId& nextPid, int& splitKey_t, int& splitPid_t) {

	RC rc;
	bool updateRoot = false;
	int newPid, siblingKey;

	//Leaf case
	if(currHeight == treeHeight) {

		BTLeafNode leaf;
		if( (rc = leaf.read(nextPid, pf)) < 0) {
			return rc;
		}

		//attempt to insert
		if( (rc = leaf.insert(key, rid) ) == 0) {
			leaf.write(nextPid, pf);
			return 0;
		}

		//if insert fails, try insert and split
		BTLeafNode sibling;
		if( (rc = leaf.insertAndSplit(key, rid, sibling, siblingKey) ) < 0) {
			return rc;
		}

		newPid = pf.endPid(); // where we will write the new sibling leaf

		// save values of split key and pid so we can propagate and insert further up in nonleafs
		splitKey_t = siblingKey;
		splitPid_t = newPid;

		// if succesful, we need to update parent and next pointers
		sibling.setNextNodePtr(leaf.getNextNodePtr());
		leaf.setNextNodePtr(newPid);

		rc = sibling.write(newPid, pf); // actually write sibling to new pid
		if (rc < 0) {
			return rc;
		}
		rc = leaf.write(nextPid, pf); // write updated current leaf to pid
		if (rc < 0) {
			return rc;
		}

		if (treeHeight == 1) {
			updateRoot = true;
		}
	} else {
		BTNonLeafNode nonLeaf;
		if( (rc = nonLeaf.read(nextPid, pf)) < 0) {
			return rc;
		}	

		int childPid = -1;
		if( (rc = nonLeaf.locateChildPtr(key, childPid)) < 0) {
			return rc;
		}

		int splitKey = -1;
		int splitPid = -1;

		rc = rec_insert(key, rid, currHeight + 1, childPid, splitKey, splitPid);
		if(rc < 0) {
			// what to do?
		}

		// we had a split earlier, so we need to insert this median value
		// into this node, or propagate it up
		if (splitPid != -1) {
			if (nonLeaf.insert(splitKey, splitPid) == 0) {
				nonLeaf.write(nextPid, pf); // insert worked fine so we can return
				return 0;
			}

			// if insert fails, try insert and split
			BTNonLeafNode sibling;
			if( (rc = nonLeaf.insertAndSplit(splitKey, splitPid, sibling, siblingKey) ) < 0) {
				return rc;
			}

			newPid = pf.endPid(); // where we will write the new sibling nonleaf

			// save values of split key and pid so we can propagate and insert further up in nonleafs
			splitKey_t = siblingKey;
			splitPid_t = newPid;

			rc = sibling.write(newPid, pf); // actually write sibling to new pid
			if (rc < 0) {
				return rc;
			}
			rc = nonLeaf.write(nextPid, pf); // write updated current nonleaf to pid
			if (rc < 0) {
				return rc;
			}

			if (treeHeight == 1) {
				updateRoot = true;
			}
		}
	}

	if (updateRoot) {
		BTNonLeafNode n_root;
		n_root.initializeRoot(nextPid, siblingKey, newPid);
		rootPid = pf.endPid();
		n_root.write(rootPid, pf);
		treeHeight++;
	}

	return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	// create a root
	if (treeHeight == 0) {
		BTLeafNode root;
		root.insert(key, rid);

		int newPid = pf.endPid();
		if (newPid == 0) {
			rootPid = 1; // 0 is used for storing index data
		} else {
			rootPid = newPid;
		}

		treeHeight = 1;
		return root.write(rootPid, pf);
	}

	int splitKey = -1, splitPid = -1;
    return rec_insert(key, rid, 1, rootPid, splitKey, splitPid);
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
		  cout << "currheight: " << currHeight << " leaf read error: " << rc << endl;
		  return rc;
		}

		if( (rc = leaf.locate(searchKey, cursor.eid)) < 0) {
		  cout << "currheight: " << currHeight << " leaf locate error: " << rc << endl; 
		  return rc;
		}

		cursor.pid = nextPid;
		return 0;
	}

	//recursive step check for errors and go down to leaf
	BTNonLeafNode nonLeaf;
	if( (rc = nonLeaf.read(nextPid, pf)) < 0) {
		cout << "curreheight: " << currHeight << " " << "nonleaf read error: " << rc << endl;
		return rc;
	}	

	if( (rc = nonLeaf.locateChildPtr(searchKey, nextPid)) < 0) {
		cout << "curreheight: " << currHeight << " " << "nonleaf locateChildPtr error: " << rc << endl;		
		return rc;
	}

	return search_tree(searchKey, cursor, currHeight + 1, nextPid);

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
	cout << "rootPid: " << rootPid << endl;
	return search_tree(searchKey, cursor, 1, rootPid); 
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

	rc = leaf.read(cursor.pid, pf);
	if(rc < 0) {
		return rc;
	}

	rc = leaf.readEntry(cursor.eid, key, rid);
	if (rc < 0) {
		return rc;
	}

	// ensure we do not increment past the number of keys available
	if (cursor.eid + 1 < leaf.getKeyCount()) {
		cursor.eid++;
	} else {
		// reset the eid but move to the next page
		cursor.eid = 0;
		cursor.pid = leaf.getNextNodePtr();
	}

    return 0;
}

PageId BTreeIndex::getRootPid() {
	return rootPid;
}

int BTreeIndex::getTreeHeight() {
	return treeHeight;
}
