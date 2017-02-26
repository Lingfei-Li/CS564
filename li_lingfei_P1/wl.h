/*
* @file wl.h
* @author Lingfei Li <lli372@wisc.edu>
* @version 1.0
*
* @section DESCRIPTION
*  Description: 
*	This is the CS564 Database Project 1 - word locator.
*	It reads input from the user, loads a file, and
*	gives the location of words according to commands.
*
*  Student Name: Lingfei Li
*  UW Campus ID: 9074068637
*  enamil: lli372@wisc.edu
*/


#include<iostream>
#include<string>
#include<sstream>
#include<algorithm>
#include<fstream>
#include<ctype.h>

using namespace std;

/**
* @brief List of integers
* @author Lingfei Li
*
* This is a class that mimics the c++ STL vector
* data structure. 
*/
class List {
private:
    /**
     * Memory space that stores the data 
     */
    int* mem;               
    /**
     * The size of the memory space allocated 
     */
    int capacity;           
    /** 
     * The current usage of the memory space 
     */
    int size;               
public:
    /** 
     * Constructor that initializes with default parameters
     */
    List() {
        capacity = 100;
        size = 0;
        mem = (int*)malloc(capacity*sizeof(int));
    }

    /** 
     * Add an integer to the list. Similar to push_back() of vector
     */
    void add(int num) {
        //Reallocate memory if space if used up
        if(size >= capacity - 1) {
            capacity *= 2;
            mem = (int*)realloc(mem, capacity*sizeof(int));
        }
        mem[size++] = num;
    }

    /**
     * Retrieve integer at a specific index
     */
    int get(int index) {
        return mem[index];
    }

    /**
     * Dump all integers in the list. Mainly for debug purpose
     */
    void dump() {
        for(int i = 0; i < size; i ++) {
            cout<<i<<": "<<mem[i]<<endl;
        }
    }
    
    /**
     * Return the number of elements in this list
     */
    int length() {
        return size;
    }

    /**
     * Destructor
     */
    void destruct() {
        free(mem);
    }
};


/**
* @brief Trie tree nodes
* @author Lingfei Li
*
* This is a class of the nodes of a basic trie tree
* This node has a field occurence to store the location
* of the words found in the input file.
*/
class TrieNode {
public:
    /**
     * Records the occurrence of the words
     * */
	List occurence;

    /**
     * The children trie nodes
     * */
	TrieNode* children[300];

    /**
     * A constructor
     * Takes no parameter. Initialize the children nodes 
     * of the new TrieNode to null
     * */
	TrieNode() {
		for (int i = 0; i < 300; i++) children[i] = NULL;
	}
};



/** 
 * Determine if a character is valid for a word
 * @param ch character to be examined
 */
bool isValidChar(char ch);

/**
 * Load the file content from the file stream
 * @param fin in-filestream
 */
TrieNode* load(ifstream& fin);
/**
 * Locate the n-th occurrence of the given word in the trie
 * @param root the root of trie
 * @param word the word to be located
 * @param n the number of occurrence to be located
 */
int locate(TrieNode* root, const string& word, int n);
/**
 * Transform all characters in a string to lower case
 * @param input input string
 */
void tolower(string& input);
/**
 * Free the trie tree memory space
 * @param root the root of the trie tree
 */
void deleteTrie(TrieNode* root);

