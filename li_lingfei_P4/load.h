#include<cstdio>
#include<iostream>
#include<sstream>
#include<string>
#include <vector>
#include <iterator>
#include <fstream>
#include"sqlite3.h"

using namespace std;

class DBOperationException {
    public:
        const char*msg;
        DBOperationException(const char* m): msg(m) {}
};

void replaceAll(string& str, const string& from, const string& to) {
    if(from.empty()) return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
}


vector<string> split(const string &s, char delim) {
    vector<string> elems;
    stringstream ss;
    ss.str(s);
    string item;
    while (getline(ss, item, delim)) {
        //escape single quote
        replaceAll(item, "'", "''");

        //replace strides with quotes
        replaceAll(item, "~", "\'");   

        elems.push_back(item);
    }
    return elems;
}


