/**
* @file wl.cpp
* @author Lingfei Li <lli372@wisc.edu>
* @version 1.0
*
* @section DESCRIPTION
*  Description: 
*	This is the CS564 Database Project 1 - word locator.
*	It reads input from the user, loads a file, and
*	gives the location of words according to commands.

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
#include"wl.h"

using namespace std;


bool isValidChar(char ch) {
	return (ch == '\'') || ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ('0' <= ch && ch <= '9');
}

void tolower(string& input) {
	for (int i = 0; i < input.length(); i++) {
		input[i] = tolower(input[i]);
	}
}

TrieNode* load(ifstream& fin) {
	TrieNode* root = new TrieNode();

	int wordNum = 1;
	string word;
	TrieNode* u = root;
	bool startWord = false;
	while (fin >> word) {
		for (int i = 0; i < word.size(); i++) {
			char ch = tolower(word[i]);		//All words are case in-sensitive
			if (isValidChar(ch) == true) {
				startWord = true;
				if (u->children[ch] == NULL) {
					u->children[ch] = new TrieNode();
				}
				u = u->children[ch];
			}
			else {
				if (startWord) {
					u->occurence.add(wordNum);
					startWord = false;
					wordNum++;
					u = root;
				}
			}
		}
		if (startWord) {
			u->occurence.add(wordNum);
			wordNum++;
			u = root;
		}
	}
	return root;
}

int locate(TrieNode* root, const string& word, int n) {
	if (root == NULL || n <= 0)
		return 0;

	TrieNode* u = root;
	for (int i = 0; i < word.size(); i++) {
		char ch = word[i];
		if (u == NULL || u->children[ch] == NULL) {
			return 0;
		}
		u = u->children[ch];
	}
	if (u->occurence.length() >= n) {
		return u->occurence.get(n - 1);
	}

	return 0;
}

void deleteTrie(TrieNode* root) {
	if (root == NULL) return;
	for (int i = 0; i < 300; i++) {
		if (root->children[i] != NULL)
			deleteTrie(root->children[i]);
	}
    root->occurence.destruct();
	delete(root);
}


/**
 * The main function
 * */
int main() {
	TrieNode* trie = NULL;      //the tree structure to be used for locator
	while (true) {
		string input;
		cout << ">";
		getline(cin, input);
		tolower(input);
		string argv[10];
        int argc = 0;
        int prevpos = 0, pos = 0;

        //Split the input string by white space
        while(true) {
            while(pos < input.length() && input[pos] == ' ') {
                pos ++;
            }
            if(pos >= input.length()) {
                break;
            }
            prevpos = pos;
            while(pos < input.length() && input[pos] != ' ') {
                pos ++;
            }
            argv[argc++] = input.substr(prevpos, pos-prevpos);
        }

        //Invalid command
		if (argc < 1) {
			continue;
		}

        //Recognize command
		string cmd = argv[0], param1, param2;
		if (cmd.compare("load") == 0 && argc == 2) {
			param1 = argv[1];

			ifstream infile(param1.c_str(), ifstream::in);
            if(trie != NULL) {
                deleteTrie(trie);
            }
			trie = load(infile);
		}
		else if (cmd.compare("locate") == 0 && argc == 3) {
			param1 = argv[1];
			param2 = argv[2];

			char* p;
			int param2_int = strtol(param2.c_str(), &p, 10);
			if (*p) {
				cout << "ERROR: Invalid command" << endl;
				continue;
			}
			int loc = locate(trie, param1, param2_int);
			if (loc == 0) {
				cout << "No matching entry" << endl;
			}
			else {
				cout << loc << endl;
			}
		}
		else if (cmd.compare("new") == 0 && argc == 1) {
			deleteTrie(trie);
			trie = NULL;
		}
		else if (cmd.compare("end") == 0 && argc == 1) {
            if(trie != NULL) {
                deleteTrie(trie);
            }
			break;
		}
		else {
			cout << "ERROR: Invalid command" << endl;
		}

	}

	return 0;
}
