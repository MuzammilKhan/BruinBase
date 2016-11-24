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
#include "BTreeIndex.h"
#include <iostream>
#include <cstdio>

using namespace std;


int main()
{

// 	key: 2342, value: Last Ride, The
// rid pid: 0, rid sid: 1key: 2634, value: Matter of Life and Death, A
// rid pid: 0, rid sid: 2key: 3992, value: Strangers on a Train
// rid pid: 0, rid sid: 3key: 2965, value: Notre Dame de Paris
// rid pid: 0, rid sid: 4key: 3084, value: Outside the Law
// rid pid: 0, rid sid: 5key: 2244, value: King Creole
// rid pid: 0, rid sid: 6key: 1578, value: G.I. Blues
// rid pid: 0, rid sid: 7keys: -1 *****DONE PRINTIN****


  // BTreeIndex test;
  // cout << "Initial Rootpid: " << test.getRootPid() << endl;
  // test.open("testIndex.idx", 'w');

  // test.insert(2342, (RecordId) {0, 1});
  // test.insert(2634, (RecordId) {0, 2});
  // test.insert(3992, (RecordId) {0, 3});
  // test.insert(2965, (RecordId) {0, 4});
  // test.insert(3084, (RecordId) {0, 5});
  // test.insert(2244, (RecordId) {0, 6});
  // test.insert(1578, (RecordId) {0, 7});


  // IndexCursor cursor;
  // //cursor.eid = 0;
  // //cursor.pid = 1; //double check these
  // int keytest;
  // RecordId ridtest;
  // cout << "keys: ";
  // while(test.readForward(cursor, keytest ,ridtest) == 0) {
  //   cout << keytest << " ";
  // }


  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}
