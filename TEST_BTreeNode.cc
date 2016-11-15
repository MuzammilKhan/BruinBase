#include "BTreeNode.h"
#include <iostream>
using namespace std;

int main()
{
    BTNonLeafNode root = new BTNonLeafNode(0);
    root.initializeRoot(1, 1, 2);

    BTNonLeafNode n1 = new BTNonLeafNode(1);
    BTNonLeafNode n2 = new BTNonLeafNode(2);

    //link nonleafnodes to have leafnode pids

    BTLeafNode l1 = new BTLeafNode(3);
    BTLeafNode l2 = new BTLeafNode(4);
    BTLeafNode l3 = new BTLeafNode(5);
    BTLeafNode l4 = new BTLeafNode(6);

    l1.setNextNodePtr(4);
    l2.setNextNodePtr(5);
    l3.setNextNodePtr(6);
    //l4 next should be set to -1 already -- NULL

    //insert statement stuff for nonleafnode and leafnode

    //print contents
    l1.print();
    l2.print()
    l3.print();
    l4.print();

    //insert and split stuff

    //print contents

    return 0;
}