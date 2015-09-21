#include <cstdlib>
#include "zlib.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <vector>
#include "mongo/client/dbclient.h"
#include <sstream>
#include <iostream>
#include <fstream>
#include "DataOperations.h"
#include <string>


using namespace std;

string itos(int i);
float htonf(float val);
void toByteArray_htonf(float value, Byte * barr);
void toByteArray_htoni(int value, Byte * barr);
void toByteArray_htons(short value, Byte * barr);
void toByteArray_htonll(long long value, Byte * barr);
void toByteArray_htond(double value, Byte * barr);
std::vector<std::string> tokenize(const std::string & str, const std::string & delim);
bool getLineData(const std::string & str, const std::string & delim, vector<string> &linevec, std::string dp);
bool readFileOfNumbersAndStrings(string source_file, vector<vector <string> > &dataVec, int &numOfRows, int &numOfCols, string dataPattern);
bool fileOfNumOrString2CompressedByteArrays(string sourceFile, string dp, vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays);
void twodVec2CompressedByteArrays(vector< vector<string> > &dataVec, int row, int col, string dp, vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays);
bool compressedDataToFile(vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays, string dp, int numOfRows, int numOfCols, string destFile);
bool fileOfNumOrString2CompressedDataFile(string sourceFile, string dp, string destFile);

