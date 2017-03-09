#include <iostream>
#include <string>
#include "string.h"
#include <sstream>
#include <vector>
#include <stack>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"
#include "btree.h"

namespace badgerdb
{
/* Assignment for structures*/

/*
template <class T>
void assignPageKeyPair( PageKeyPair<T>& dst, PageKeyPair<T>& src) {
    assignPageKeyPair<T>(dst, src.pageNo, src.key);
}
template <class T>
void assignPageKeyPair( PageKeyPair<T>& dst, PageId pageNo, T key) {
    dst.pageNo = pageNo;
    assignKey<T>(dst, key);
}

template <class T>
void assignRIDKeyPair( RIDKeyPair<T>& dst, RIDKeyPair<T>& src) {
    assignRIDKeyPair<T>(dst, src.rid, src.key);
}

template <class T>
void assignRIDKeyPair( RIDKeyPair<T>& dst, RecordId rid, T key) {
    dst.rid = rid;
    assignKey<T>(dst, key);
}
*/


}
