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
#include "DataMovement.h"
#include <string>
#include <sstream>

using namespace mongo;
using namespace std;




class MongoC {
public:
	MongoC(string addr, string port);
	void mongoDbConnect(string addr, string port);
	void saveDataState(string database, string collection, string instanceId, long long customerId, long long startTime, long long endTime);
	void byteArrays2Mongo(vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays, string database, string collection,  string instanceId, string dp, long long startTime, long long endTime, int customerId);
	void file2Mongo(string fileName, string database, string collection,  string instanceId, long long startTime, long long endTime, int customerId);
	long long getLatestDate(string database, string collection, string instanceId, long long customerId);
private:
	string address;
	string port;
	mongo::DBClientConnection * conn;
	mongo::ScopedDbConnection * poolPtr;
	BSONObj metaDataObj;
	
};

