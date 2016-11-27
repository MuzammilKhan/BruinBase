/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <limits.h>
#include <set>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"


using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  //fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

// attr:
// 1 = key
// 2 = value
// 3 = *
// 4 = COUNT(*)
RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  fprintf(stderr, "\n*****SELECT DEBUG PRINTS******");

  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  BTreeIndex tree;

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  rid.pid = rid.sid = 0;
  count = 0;
  //fprintf(stderr, "Debug: rid.pid: %d rid.sid= %d\n", rid.pid, rid.sid);

  // struct SelCond {
  //   int attr;       // 1 means "key" attribute. 2 means "value" attribute.
  //   enum Comparator { EQ, NE, LT, GT, LE, GE } comp;
  //   char* value;    // the value to compare
  // };

  // conditions in cond must be ANDed together
  // iterate through cond and find tightest bound
  // for both keys and values, we either will have: 
  //  1. a specific value to be equal to
  //  2. a min -> max range (either could be inclusive or not), with
  //    a) value(s) to not be equal to
  //    b) nothing else extra
  // can have contradictions, resulting in no tuples
  //  1. an EQ outside of the specified range
  //  2. an EQ and NQ on same value
  //  3. contradicting ranges
  // the B+ tree gives us no benefit if:
  //  1. there are no key conds
  // additional gains
  //  1. if no value conds, save reading record file

  bool valConds = false; // assume no value conditions initally
  int k_min = INT_MIN;
  int k_max = INT_MAX;
  string v_min = "";
  string v_max = "";
  bool k_min_inclusive = true; // assume initially GE
  bool k_max_inclusive = true; // assume initially LE
  bool v_min_inclusive = true; // assume initially GE
  bool v_max_inclusive = true; // assume initially LE
  int k_eq; // key must be equal to this
  string v_eq; // value must be equal to this
  bool k_eq_set = false;
  bool v_eq_set = false;
  bool v_min_set = false; // need this since there is no min string value
  bool v_max_set = false; // need this since there is no max string value

  SelCond cur_cond;
  int cur_attr; // attr of the current cond
  bool contradiction = false;

  set<string> value_ne;
  set<int> key_ne;

  // variables for deciding if there are only NE coniditions, so we use normal select
  bool other_than_ne = false;
  bool ne_set = false;
  
  for (int i = 0; i < cond.size(); i++) {
    cur_cond = cond[i];
    cur_attr = cur_cond.attr; // 1 for key, 2 for value
    //fprintf(stderr, "value in cond %d: %d\n", i, cur_v);

    switch (cur_cond.comp) {
        case SelCond::EQ:
        case SelCond::GE:
        case SelCond::LE:
        case SelCond::GT:
        case SelCond::LT:
          other_than_ne = true;
        case SelCond::NE:
          ne_set = true;
    }

    // dealing with a key constraint
    if (cur_attr == 1) {
      int cur_v = atoi(cur_cond.value);
      switch (cur_cond.comp) {
        case SelCond::EQ:
          // set equality variable if not set, check for contradiction
          if ((k_min_inclusive && cur_v < k_min) ||
              (!k_min_inclusive && cur_v <= k_min) ||
              (k_max_inclusive && cur_v > k_max) ||
              (!k_max_inclusive && cur_v >= k_max)) {
            contradiction = true;
          } else if (k_eq_set) {
            if (k_eq != cur_v) {
              contradiction = true;
            }
            // otherwise this is an equality constraint for the same value, so its ok
          } else { // a new equality constraint
            k_eq_set = true;
            k_eq = cur_v;
          }
          break;
        case SelCond::NE:
          // we can deal with this later as long as it doesn't contradict EQ
          if (k_eq_set && cur_v == k_eq) {
           contradiction = true;
          } else {
            key_ne.insert(cur_v);
          }
          break;
        case SelCond::GT:
          // possibly increase minimum and possibly mark it as noninclusive
          if (cur_v >= k_min) {
            if(k_eq_set && k_eq <= cur_v) {
              contradiction = true;
              break;
            }
            //fprintf(stderr, "IN KEY GT CASE, setting min as: %d\n", cur_v);
            k_min = cur_v;
            k_min_inclusive = false;
          }
          break;
        case SelCond::LT:
          // possibly decrease maximum and possibly mark it as noninclusive
          if (cur_v <= k_max) {
            if(k_eq_set && k_eq >= cur_v) {
              contradiction = true;
              break;
            }
            k_max = cur_v;
            k_max_inclusive = false;
          }
          break;
        case SelCond::GE:
          // possibly increase minimum and possibly mark it as inclusive
          if (cur_v > k_min) {
            if(k_eq_set && k_eq < cur_v) {
              contradiction = true;
              break;
            }
            k_min = cur_v;
            k_min_inclusive = true;
          }
          break;
        case SelCond::LE:
          // possibly decrease maximum and possibly mark it as inclusive
          if (cur_v < k_max) {
            if(k_eq_set && k_eq > cur_v) {
              contradiction = true;
              break;
            }
            k_max = cur_v;
            k_max_inclusive = true;
          }
          break;
      }

      // range contradiction
      if (k_min > k_max) contradiction = true;

      // e.g. (1, 1] or [1,1) has no possible values
      if ((!k_min_inclusive || !k_max_inclusive) && k_min == k_max) contradiction = true;
    } else { // dealing with a value constraint
      char* cur_v = cur_cond.value;
      valConds = true; // there is some valid value constraint
      switch (cur_cond.comp) {
        case SelCond::EQ:
          // set equality variable if not set, check for contradiction
          if ((v_min_set && v_min_inclusive && strcmp(cur_v, v_min.c_str()) < 0)  ||
             (v_min_set && !v_min_inclusive && strcmp(cur_v, v_min.c_str()) <= 0)  ||
             (v_max_set && v_max_inclusive && strcmp(cur_v, v_max.c_str()) > 0) ||
             (v_max_set && !v_max_inclusive && strcmp(cur_v, v_max.c_str()) >= 0)) {
            contradiction = true;
          } else if (v_eq_set) {
            if (strcmp(v_eq.c_str(), cur_v) != 0) {
              contradiction = true;
            }
            // otherwise this is an equality constraint for the same value, so its ok
          } else { // a new equality constraint
            v_eq_set = true;
            v_eq = cur_v;
          }
          break;
        case SelCond::NE:
          // we can deal with this later as long as it doesn't contradict EQ
          if (v_eq_set && strcmp(cur_v, v_eq.c_str()) == 0) {
            contradiction = true;
          } else {
            value_ne.insert(string(cur_v));
          }
          break;
        case SelCond::GT:
          // possibly increase minimum and possibly mark it as noninclusive
          if (!v_min_set || strcmp(cur_v, v_min.c_str()) > 0) {
            if (v_eq_set &&  strcmp(v_eq.c_str(), cur_v) <= 0 ) {
              contradiction = true;
              break;
            }
            v_min = cur_v;
            v_min_inclusive = false;
            v_min_set = true;
          }
          break;
        case SelCond::LT:
          // possibly decrease maximum and possibly mark it as noninclusive
          if (!v_max_set || strcmp(cur_v, v_max.c_str()) <= 0) {
            if (v_eq_set &&  strcmp(v_eq.c_str(), cur_v) >= 0 ) {
              contradiction = true;
              break;
            }
            v_max = cur_v;
            v_max_inclusive = false;
            v_max_set = true;
          }
          break;
        case SelCond::GE:
          // possibly increase minimum and possibly mark it as inclusive
          if (!v_min_set || strcmp(cur_v, v_min.c_str()) > 0) {
            if (v_eq_set &&  strcmp(v_eq.c_str(), cur_v) < 0 ) {
              contradiction = true;
              break;
            }
            v_min = cur_v;
            v_min_inclusive = true;
            v_min_set = true;
          }
          break;
        case SelCond::LE:
          // possibly decrease maximum and possibly mark it as inclusive
          if (!v_max_set || strcmp(cur_v, v_max.c_str()) < 0) {
            if (v_eq_set &&  strcmp(v_eq.c_str(), cur_v) > 0 ) {
              contradiction = true;
              break;
            }
            v_max = cur_v;
            v_max_inclusive = true;
            v_max_set = true;
          }
          break;
      }

      // range contradiction
      if ((v_min_set && v_max_set) && 
          (strcmp(v_min.c_str(), v_max.c_str()) > 0)) {
        contradiction = true;
      }

      // e.g. (1, 1] or [1,1) has no possible values
      if ((v_min_set && v_max_set) && 
          (!v_min_inclusive || !v_max_inclusive) && 
          (strcmp(v_min.c_str(), v_max.c_str()) == 0)) {
        contradiction = true;
      }
    }

    //fprintf(stderr, "KEY MIN: %d, KEY MAX: %d, KMIN INCLUSIVE: %d, KMAX INCLUSIVE: %d, CONTR: %d\n", 
    //        k_min, k_max, k_min_inclusive, k_max_inclusive, contradiction);

    if (contradiction) {
      rc = rf.close();
      // if (rc < 0) {
      //   return rc;
      // }
      return RC_INVALID_ATTRIBUTE; // CHECK if this is correct return attr
    }
  }

  rc = tree.open(table + ".idx", 'r');

  //fprintf(stderr, "Debug: attempt to open tree %d\n", rc);
  // do normal select routine if index file not found or if only NE is set
  if ((rc < 0) || ((!other_than_ne) && ne_set)){
    fprintf(stderr, "Debug: do normal select routine\n");
    // scan the table file from the beginning
    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
          case 1:
            diff = key - atoi(cond[i].value);
            break;
          case 2:
            diff = strcmp(value.c_str(), cond[i].value);
            break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
          case SelCond::EQ:
            if (diff != 0) goto next_tuple;
            break;
          case SelCond::NE:
            if (diff == 0) goto next_tuple;
            break;
          case SelCond::GT:
            if (diff <= 0) goto next_tuple;
            break;
          case SelCond::LT:
            if (diff >= 0) goto next_tuple;
            break;
          case SelCond::GE:
            if (diff < 0) goto next_tuple;
            break;
          case SelCond::LE:
            if (diff > 0) goto next_tuple;
            break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      // fprintf(stderr, "Debug: Check if we should print tuple\n");
      switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }
  } else { // we have an index, so use that
    IndexCursor c; // to iterate through tree
    c.eid = 0; // set default values
    c.pid = 1; // page 0 is index
    //fprintf(stderr, "Debug: using index\n");
    // fprintf(stderr, "Debug: rid: %d sid: %d\n", rid.sid, rid.pid);
    // need to locate entry point into tree
    if (k_eq_set) {
      tree.locate(k_eq, c);
    } else if (k_min_inclusive) {
      tree.locate(k_min, c);
    } else if (!k_min_inclusive) {
      tree.locate(k_min + 1, c);
    } else {
      tree.locate(INT_MIN, c); // maybe start at 0?
    }
    //fprintf(stderr, "Debug: rid: %d sid: %d\n", rid.sid, rid.pid);
    bool done = false, next_iteration = false;

    // keep reading while there are elements and we haven't yet terminated
    //fprintf(stderr, "Debug: keep reading\n");
    while (rc = tree.readForward(c, key, rid) == 0) {
      // check if key is within bounds
      // fprintf(stderr, "Debug: in while\n");
      // fprintf(stderr, "   KEY VALUE: %d\n", key);
      if ((k_eq_set && key != k_eq) ||
         (k_min_inclusive && key < k_min)  ||
         (!k_min_inclusive && key <= k_min)  ||
         (k_max_inclusive && key > k_max) ||
         (!k_max_inclusive && key >= k_max)) {
        break;
      }

      if (key_ne.find(key) != key_ne.end()) {
        // fprintf(stderr, "Debug: Value within NE variables, continuing\n");
        continue;
      }
      // fprintf(stderr, "Debug: after keep reading: rc = %d\n", rc);
      // fprintf(stderr, "Debug: rid.pid: %d, rid.sid: %d\n", rid.pid, rid.sid);

      // if there are no value conditions, and we don't need to print anything, we can just continue here
      if (!valConds && attr == 4) {
        count++;
        continue;
      }
        
      // read in value from record file
      rc = rf.read(rid, key, value);
      if (rc < 0) {
	     //fprintf(stderr, "Debug: read record file failed rc: %d rid.sid: %d rid.pid: %d\n", rc,rid.sid, rid.pid);
        break; // something went wrong
      }

      const char* con_v = value.c_str();

      // check if value is within bounds
      if ((v_eq_set && strcmp(con_v, v_eq.c_str()) != 0) ||
         (v_min_set && v_min_inclusive && strcmp(con_v, v_min.c_str()) < 0)  ||
         (v_min_set && !v_min_inclusive && strcmp(con_v, v_min.c_str()) <= 0)  ||
         (v_max_set && v_max_inclusive && strcmp(con_v, v_max.c_str()) > 0) ||
         (v_max_set && !v_max_inclusive && strcmp(con_v, v_max.c_str()) >= 0)) {
        // fprintf(stderr, "Debug: check within bounds and continuing\n");
	continue;
      }

      // need to check that value is not within the NE variables
      if (value_ne.find(value) != value_ne.end()) {
	// fprintf(stderr, "Debug: Value within NE variables, continuing\n");
        continue;
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;
      // fprintf(stderr, "Debug: Index -- Check if there is a tuple to print\n");
      // print the tuple 
      switch (attr) {
        case 1:  // SELECT key
          fprintf(stdout, "%d\n", key);
          break;
        case 2:  // SELECT value
          fprintf(stdout, "%s\n", value.c_str());
          break;
        case 3:  // SELECT *
          fprintf(stdout, "%d '%s'\n", key, value.c_str());
          break;
      }
    }

    tree.close();
  }

  // enter this if everything was ok
  if (!contradiction && rc == 0) {
    // do stuff
    // print matching tuple count if "select count(*)"
    if (attr == 4) {
      fprintf(stdout, "%d\n", count);
    }
    rc = 0;
  }

  fprintf(stderr, "\n*******DONE SELECT DEBUG******");

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  fprintf(stderr, "*****LOAD DEBUG PRINTS******\n");
  /* your code here */
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning
  ifstream ifs; //input loadfile filestream

  //exit status variables
  RC     rc;

  //Insertion variables
  int    key;     
  string value;
  int    count; //line number in load file
  string line;
  BTreeIndex btree;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'w')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  //open loadfile
  try {
    ifs.open(loadfile.c_str(), ifstream::in);
  } catch(...) {
      rf.close();
    return RC_FILE_OPEN_FAILED;
  }

  //open index
  if(index && (rc = btree.open(table + ".idx", 'w')) < 0) {
    fprintf(stderr, "Error: failed to open b+tree index for table %s \n", table.c_str());
    rf.close();
    ifs.close(); 
    return rc;
  }

  //insert data 
  while(! ifs.eof()) {
    getline(ifs, line);
    if(line == "")
      break;

    if((rc = parseLoadLine(line, key, value)) < 0) {
      fprintf(stderr, "Error: Unable to parse input file %s at line %d\n", loadfile.c_str(), count);
      rf.close();
      ifs.close();
      if(index){ btree.close();}
      return rc;
    }

    // fprintf(stderr, "key: %d, value: %s\n", key, value.c_str());

    if((rc = rf.append(key,value,rid)) < 0) {
      fprintf(stderr, "Error: Unable to insert data to table %s at line %d of %s\n", table.c_str(), count, loadfile.c_str());
      rf.close();
      ifs.close();
      if(index){ btree.close();}
      return rc;
    }

    //fprintf(stderr, "rid pid: %d, rid sid: %d", rid.pid, rid.sid );

    if(index && (rc = btree.insert(key,rid)) < 0) {
    fprintf(stderr, "Error: Unable to insert data into index for input file %s at line %d\n", loadfile.c_str(), count);
      rf.close();
      ifs.close();
      btree.close();
      return rc;
    }
    count++;
  }

  // if(index) {
  //   fprintf(stderr, "keys: ");
  //   IndexCursor cursor;
  //   cursor.eid = 0;
  //   cursor.pid = 1; //double check these
  //   int keytest;
  //   RecordId ridtest;
  //   while(btree.readForward(cursor, keytest ,ridtest) == 0) {
  //     fprintf(stderr, "%d ",keytest);
  //   }
  // }

    fprintf(stderr, "\n*****DONE PRINTING LOAD****\n\n");

  rf.close();
  ifs.close();
  if (index) {
    btree.close();
  }
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
