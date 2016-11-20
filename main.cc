/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeNode.h"
#include <cstdio>
#include <iostream>

using namespace std;

int main()
{

//TESTING BELOW IS FOR BTreeNode
  
  cout << sizeof(RecordId) + sizeof(int) << endl; //size 12
  cout << sizeof(PageId) << endl; //size 4
 
  

  //Initialize new leaf node
  BTLeafNode poop;
  cout << "Initial key count: " << poop.getKeyCount() << endl;
  
  int zeroPls;
  
  
  //Try stuffing our node with (key, rid) pairs until node is full
  RecordId poopRid = {1,2};
  for(int i=0; i<86; i++)
  {
	zeroPls = poop.insert(5, poopRid);
	cout << "Am I zero? " << zeroPls << endl;
  }
	
  cout << "Final key count: " << poop.getKeyCount() << endl;
  
  
  
  //Test location by finding key with 5
  int eid = -1;
  poop.locate(5, eid);
  cout << "Found key=5 at eid: " << eid << endl;
  
  //Test location by finding key with 6 (doesn't exist - should be -1)
  int eid2 = -1;
  poop.locate(6, eid2);
  cout << "Found key=6 at eid2: " << eid2 << endl;
  
  //Test readEntry by reading in key=5's key/rid
  int key = -1;
  RecordId rid;
  rid.pid = -1;
  rid.sid = -1;
  poop.readEntry(0, key, rid);
  cout << "Entry at specified eid has key=" << key << " and pid=" << rid.pid << " and sid=" << rid.sid << endl;
  
  //Test readEntry for more indexes
  poop.readEntry(1, key, rid);
  cout << "Entry at specified eid has key=" << key << " and pid=" << rid.pid << " and sid=" << rid.sid << endl;
  poop.readEntry(85, key, rid);
  cout << "Entry at specified eid has key=" << key << " and pid=" << rid.pid << " and sid=" << rid.sid << endl;
  
  //Test next pointers
  PageId nextPid = poop.getNextNodePtr(); //should return an error
  cout << "The next (invalid) pid is: " << nextPid << endl;
  
  poop.setNextNodePtr(999);
  cout << "After setting it, the next pid is: " << poop.getNextNodePtr() << endl;

  //Test insertAndSplit by putting in a new (key, rid) pair
  BTLeafNode poop2;
  int poop2Key = -1;
  poop.insertAndSplit(100, ((RecordId){101, 102}), poop2, poop2Key);
  cout << "The first entry in poop2 has key (should be 5): " << poop2Key << endl;
  
  cout << "poop has numKeys " << poop.getKeyCount() << " and poop2 has numKeys " << poop2.getKeyCount() << endl;
  
  //Nothing should change if we don't meet the conditions to split
  zeroPls = poop.insertAndSplit(100, (RecordId){101, 102}, poop2, poop2Key);
  cout << "poop has numKeys " << poop.getKeyCount() << " and poop2 has numKeys " << poop2.getKeyCount() << " (should be same)" << endl;
  cout << "zeroPls should not be zero pls: " << zeroPls << endl;
  
  cout << "--------------------------------------------------" << endl;
  //----------------------------------------------------------------------------------------------------
  
  BTLeafNode blah;
  BTLeafNode blah2;
  blah.insert(1, (RecordId){12,23});
  blah.insert(10, (RecordId){34,45});
  blah.insert(100, (RecordId){56,67});
  blah.insert(1000, (RecordId){78,89});
  
  //Do a quick test on locate
  int blahEid = -1;
  blah.locate(0, blahEid);
  cout << "0 belongs at eid: " << blahEid << endl;
  blah.locate(1, blahEid);
  cout << "1 belongs at eid: " << blahEid << endl;
  blah.locate(5, blahEid);
  cout << "5 belongs at eid: " << blahEid << endl;
  blah.locate(15, blahEid);
  cout << "15 belongs at eid: " << blahEid << endl;
  blah.locate(150, blahEid);
  cout << "150 belongs at eid: " << blahEid << endl;
  blah.locate(1500, blahEid);
  cout << "1500 belongs at eid: " << blahEid << endl;
  
  //Set up insertAndSplit on leaf nodes test
  int key2 = -1;
  RecordId rid2;
  rid2.pid = -1;
  rid2.sid = -1;
  
  blah.readEntry(0, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(1, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(2, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(3, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;

  //This should stop after we fill up the entire blah buffer (produces maybe 4 errors or so)
  for(int j=0; j<85; j++)
	blah.insert(101, (RecordId){0,0});
  
  int blah2Key = -1;
  blah.insertAndSplit(99, (RecordId){9000, 9001}, blah2, blah2Key);
  
  cout << "blah has numKeys " << blah.getKeyCount() << " and blah2 has numKeys " << blah2.getKeyCount() << endl;
  cout << "blah2's first entry is now key: " << blah2Key << endl;
  
  int eid3 = -1;
  blah.locate(99, eid3);
  
  cout << "blah contains key=99 at eid: " << eid3 << endl;
  
  blah.readEntry(0, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(1, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(2, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  blah.readEntry(3, key2, rid2);
  cout << "Entry at specified eid has key=" << key2 << " and pid=" << rid2.pid << " and sid=" << rid2.sid << endl;
  
  cout << "--------------------------------------------------" << endl;
  //----------------------------------------------------------------------------------------------------
  
  BTNonLeafNode root;
  
  //Initialize root and verify number of keys
  root.initializeRoot(0,99,1);
  cout << "root node has numKeys: " << root.getKeyCount() << endl;
  
  //Insert to root and verify number of keys
  root.insert(999, 2);
  cout << "After insert, root node has numKeys: " << root.getKeyCount() << endl;
  
  //Try to insertAndSplit (this should fail)
  BTNonLeafNode sibling;
  int median = -1;
  root.insertAndSplit(9999, 3, sibling, median);
  cout << "After insertAndSplit, root node has numKeys (should be same): " << root.getKeyCount() << endl;
  cout << "Median: " << median << endl;
  
  //Check child pointers
  PageId rootPid = -1;

  root.print();
  
  root.locateChildPtr(50, rootPid);
  cout << "50 has child pointer: " << rootPid << endl;
  
  root.locateChildPtr(500, rootPid);
  cout << "500 has child pointer: " << rootPid << endl;
  rootPid = -1;
  root.locateChildPtr(5000, rootPid);
  cout << "5000 has child pointer: " << rootPid << endl;
  
  for(int k=0; k<5; k++)
	root.insert(9,4);
  
  cout << "root node has numKeys: " << root.getKeyCount() << endl;
  
  for(int k=0; k<121; k++)
	root.insert(9999,5);
	
  cout << "root node has numKeys: " << root.getKeyCount() << endl;
  
  root.insertAndSplit(99999, 6, sibling, median);
  cout << "After insertAndSplit, root node has numKeys: " << root.getKeyCount() << endl;
  cout << "After insertAndSplit, sibling node has numKeys: " << sibling.getKeyCount() << endl;
  cout << "Median: " << median << endl;
  
  //Let's test for median more accurately
  BTNonLeafNode root2;
  root2.initializeRoot(0,2,1);
  for(int k=0; k<127; k++)
	root2.insert(2*(k+2),2);

  //Currently, root2 looks something like  (2, 4, 6, 8, ..., 126, 128, 130, ..., 252, 254)
  //Median should be 128 if we insert 127 [reason: adding 127 bumps 128 up into the middle key]
  //Median should be 129 if we insert 129 [reason: 129 itself becomes the middle most key]
  //Median should be 130 if we insert 131 [reason: adding 131 bumps 130 down into the middle key]
  
  median = -1;
  BTNonLeafNode sibling2;
  root2.insertAndSplit(131, 6, sibling2, median); //Replace the key to be inserted with the numbers above to test
  cout << "After insertAndSplit, root2 node has numKeys: " << root2.getKeyCount() << endl;
  cout << "After insertAndSplit, sibling2 node has numKeys: " << sibling2.getKeyCount() << endl;
  cout << "Median: " << median << endl;
  // run the SQL engine taking user commands from standard input (console).

  SqlEngine::run(stdin);

  return 0;
}
