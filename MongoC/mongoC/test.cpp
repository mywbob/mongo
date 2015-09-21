#include <iostream>
#include <fstream>
#include <cstdlib>
#include "zlib.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <vector>
#include <sstream>
#include "mongo/client/dbclient.h"
#include "DataMovement.h"
#include "DataOperations.h"
#include "MongoC.h"


using namespace mongo;
using namespace std;
//g++ DataOperations.cpp MongoC.cpp DataMovement.cpp test.cpp -pthread -I /Users/mywbob/Documents/mongo-cxx-driver-nightly/src -I /Users/mywbob/Documents/mongo-cxx-driver-nightly/src/mongo -I./ -lmongoclient -lboost_thread-mt -lboost_system-mt -lboost_filesystem-mt -L/Users/mywbob/Documents/mongo-cxx-driver-nightly -L./ -L/Users/mywbob/Documents/zlib-1.2.8 -lz -o testMongo -w

void test1() {
	MongoC mongo = MongoC("128.4.30.3", "29010");	
	vector<Byte *> byteArrays;// cols of data
	vector<int> lengthOfCompressedArrays; //len for each col
	string dp = "fdlist"; //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change here
	fileOfNumOrString2CompressedByteArrays("test2.txt", dp, byteArrays, lengthOfCompressedArrays);//allocate byteArrays in here

	string database("testdb");
	string collection("testcoll");
	string instanceId("1234");
	long long startTime = 2013;
	long long endTime = 2014;
	int customerId = 777;
	mongo.byteArrays2Mongo(byteArrays, lengthOfCompressedArrays, database, collection, instanceId, dp, startTime, endTime, customerId);// use byteArrays here then delete it inside here
	
}

void test2() {
	MongoC mongo = MongoC("128.4.30.3", "29010");
	string database("testdb");
	string collection("testcoll");
	string instanceId("5678");
	long long startTime = 1;
	long long endTime = 2;
	int customerId = 999;
	fileOfNumOrString2CompressedDataFile("test1.txt", "fsfifffdtffdlff", "test.cmp");
	mongo.file2Mongo("test.cmp", database, collection,  instanceId, startTime, endTime, customerId);// use byteArrays here then should be deleted after
}

/*
int main() {
	test1();
	test2();
return 0;
}
*/

/*
int main(int argc, char* argv[]) {
	//system_cl_i-5348667d_169_1393533051_1394638071.txt
	
	MongoC mongo = MongoC("127.0.0.1", "27890");	
	
	if (string(argv[1]) == "-s") {//care
		vector<Byte *> byteArrays;// cols of data
		vector<int> lengthOfCompressedArrays; //len for each col
		string dp = "sisisiiis"; //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change here
	
		//argv[2] is the file name 
		fileOfNumOrString2CompressedByteArrays(argv[2], dp, byteArrays, lengthOfCompressedArrays);//allocate byteArrays in here
	
		string database("system_cl");
		string collection("coll");
		string instanceId = argv[3];//"i-5348667d";
		int customerId = (int) atoi(argv[4]);
		long long startTime = (long long) atoi(argv[5]);
		long long endTime = (long long) atoi(argv[6]);
		mongo.byteArrays2Mongo(byteArrays, lengthOfCompressedArrays, database, collection, instanceId, dp, startTime, endTime, customerId);// use byteArrays here then delete it inside here
	}
	if (string(argv[1]) == "-l") {
		string database("system_cl");//getLatestDate will find system_cl_instance_state
		string collection("coll");
		long long latest = mongo.getLatestDate(database, collection, argv[2], (long long) atoi(argv[3]));
		cout << latest; //print to stdout, so the python can get it 
	}
	
	return 0;
}
*/


int main(int argc, char* argv[]) {
	//system_cl_i-5348667d_169_1393533051_1394638071.txt
	
	MongoC mongo = MongoC("127.0.0.1", "27890");	
	
	if (string(argv[1]) == "-s") {//care
		vector<Byte *> byteArrays;// cols of data
		vector<int> lengthOfCompressedArrays; //len for each col
		string dp = "siiiiillsss"; //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change here
	
		//argv[2] is the file name 
		fileOfNumOrString2CompressedByteArrays(argv[2], dp, byteArrays, lengthOfCompressedArrays);//allocate byteArrays in here
	
		string database("system_ofl");
		string collection("coll");
		string instanceId = argv[3];//"i-5348667d";
		int customerId = (int) atoi(argv[4]);
		long long startTime = (long long) atoi(argv[5]);
		long long endTime = (long long) atoi(argv[6]);
		mongo.byteArrays2Mongo(byteArrays, lengthOfCompressedArrays, database, collection, instanceId, dp, startTime, endTime, customerId);// use byteArrays here then delete it inside here
	}
	if (string(argv[1]) == "-l") {
		string database("system_ofl");//getLatestDate will find system_cl_instance_state
		string collection("coll");
		long long latest = mongo.getLatestDate(database, collection, argv[2], (long long) atoi(argv[3]));
		cout << latest; //print to stdout, so the python can get it 
	}
	
	return 0;
}



/*
int main(int argc, char* argv[]) {
	//system_cl_i-6fe2e843_169_1391535298_1392054974
	
	MongoC mongo = MongoC("128.4.30.3", "29099");	
	vector<Byte *> byteArrays;// cols of data
	vector<int> lengthOfCompressedArrays; //len for each col
	string dp = "sisisiiis"; //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!change here
	
	
	fileOfNumOrString2CompressedByteArrays("system_cl_i-6fe2e843_169_1391535298_1392054974.txt", dp, byteArrays, lengthOfCompressedArrays);//allocate byteArrays in here
	
	string database("system_cl");
	string collection("coll");
	string instanceId = "i-6fe2e843";
	long long startTime = 1;//(long long) atoi(fieldVec[4].c_str());
	long long endTime = 2;//(long long) atoi(fieldVec[5].c_str());
	int customerId = 3;//(int) atoi(fieldVec[3].c_str());
	mongo.byteArrays2Mongo(byteArrays, lengthOfCompressedArrays, database, collection, instanceId, dp, startTime, endTime, customerId);// use byteArrays here then delete it inside here
	
	return 0;
}
*/





