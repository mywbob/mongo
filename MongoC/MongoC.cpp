#include "MongoC.h"

string INSTANCE_ID = "instance_id";
string CUSTOMER_ID = "customer_id";
string NUM_OF_ROWS = "num_of_rows";
string NUM_OF_COLS = "num_of_cols";
string END_TIME = "end_time";
string START_TIME = "start_time";
string READY_TO_PROCESS = "ready_to_process";
string TIME_OF_MOST_RECENTLY_PROCESSED = "most_recently_processed";
string DATE_ARCHIVED_TO = "archived_to"; 
string SYSTEM_CPU_INFO_EXISTS = "system_cpu_info_exists";
string IS_MICRO = "is_micro";
string ASSET_ID = "asset_id";
string DATA_PATTERN = "data_pattern";
string STRING_LEN = "string_length";


//MongoC::MongoC(string addr, string port, informationAboutWhatToDoWhenUploadFails) {
MongoC::MongoC(string addr, string port) {
	this->address = addr;
	this->port = port;	
	mongoDbConnect(addr,port);
}



void MongoC::mongoDbConnect(string addr, string port) {//reconnect
	ScopedDbConnection* pool;
	string hostInfo = addr + ":" + port;
	int numOfTry = 1000;
	int sleepTime = 3;
	while(numOfTry) {
		numOfTry--;
		try {
			pool = new ScopedDbConnection(hostInfo);
			numOfTry = 0;
		}
		catch(mongo::UserException& ue)
		{
			cout << "UserException: " << ue.toString() << endl;
			cout << numOfTry << " try left to exit" << endl;
			sleep(sleepTime);
			if (numOfTry == 0) exit (EXIT_FAILURE);
		}
		catch(mongo::MsgAssertionException& ex)
		{
			cout << "MsgAssertionException: " << ex.toString() << endl;
			cout << numOfTry << " try left to exit" << endl;
			sleep(sleepTime);
			if (numOfTry == 0) exit (EXIT_FAILURE);
		}
		catch(mongo::DBException& ex)
		{
			cout << "DBException: " << ex.toString() << endl;
			cout << numOfTry << " try left to exit" << endl;
			sleep(sleepTime);
			if (numOfTry == 0) exit (EXIT_FAILURE);
		}
		catch(std::exception& ex)
		{
			cout << "std::exception: " << ex.what() << endl;
			cout << numOfTry << " try left to exit" << endl;
			sleep(sleepTime);
			if (numOfTry == 0) exit (EXIT_FAILURE);
		}
		catch(...)
		{
			cout << "Unknown" << endl;
			cout << numOfTry << " try left to exit" << endl;
			sleep(sleepTime);
			if (numOfTry == 0) exit (EXIT_FAILURE);
		}
	}
	this->conn = (mongo::DBClientConnection*)pool->get();
	this->poolPtr = pool;//so i can do done() before ScopedDbConnection go out of scope, so we can reuse connection
}


void MongoC::file2Mongo(string fileName, string database, string collection,  string instanceId, long long startTime, long long endTime, int customerId) {

	//read the byte_array file
	ifstream in;
	in.open(fileName.c_str(), ios::in | ios::binary);
	if (in.is_open())
	{
		cout << "File successfully open" << endl;
	}
	else
	{
		cout << "Error opening file" << endl;
		return;
	}
	vector<Byte *> byteArrays;
	vector<int> lengthOfCompressedArrays;
	
	int versionNum, dpLen;
	
	//read version
	in.read((char *)&versionNum, sizeof(int));
	
	//read data pattern len
	in.read((char *)&dpLen, sizeof(int));
	
	char charsInDp[dpLen];
	
	//read data pattern
	in.read((char *)&charsInDp, dpLen);
	
	string dp(charsInDp, dpLen);

	int numOfCols = dpLen;
	for (int i=0;i<numOfCols;i++) {
		int templen;
		//read data len
		in.read((char *)&templen, sizeof(int));
		lengthOfCompressedArrays.push_back(templen);
		
		Byte * temparr = new Byte[templen];
		//read data
		in.read((char *)&temparr[0], templen * sizeof(Byte));
		byteArrays.push_back(temparr);
	}
	in.close();
	
	byteArrays2Mongo(byteArrays, lengthOfCompressedArrays, database, collection, instanceId, dp, startTime, endTime, customerId);// byteArrays delete inside
	
}






void MongoC::byteArrays2Mongo(vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays, string database, string collection, string instanceId, string dp, long long startTime, long long endTime, int customerId) {
		
	BSONObjBuilder b;	
	BSONArrayBuilder arrb;
	BSONArray arr;
	//the geneal info
	b.append(INSTANCE_ID , instanceId);
	b.append(START_TIME , startTime);
	b.append(END_TIME, endTime);
	b.append(CUSTOMER_ID, customerId);
	b.append(DATA_PATTERN, dp);
		
	
	
	//the compressed col data
	for (int i=0;i<byteArrays.size();i++) {
		b.appendBinData("binData" + itos(i), lengthOfCompressedArrays[i], BinDataGeneral, byteArrays[i]);
	}
	
	
	
	this->metaDataObj = b.obj();
	string dbAndColl = database + "." + collection;

	bool ISFAILED;
	do {
		try {
			conn->insert(dbAndColl, metaDataObj);
			ISFAILED = false;
		}
		catch(mongo::UserException& ue)
		{
			ISFAILED = true;
			cout << "UserException: " << ue.toString() << endl;
			cout << "trying to reconnect" << endl;
			mongoDbConnect(this->address, this->port);
		}
		catch(mongo::MsgAssertionException& ex)
		{
			ISFAILED = true;
			cout << "MsgAssertionException: " << ex.toString() << endl;
			cout << "trying to reconnect" << endl;
			mongoDbConnect(this->address, this->port);
		}
		catch(mongo::DBException& ex)
		{
			ISFAILED = true;
			cout << "DBException: " << ex.toString() << endl;
			cout << "trying to reconnect" << endl;
			mongoDbConnect(this->address, this->port);
		}
		catch(std::exception& ex)
		{
			ISFAILED = true;
			cout << "std::exception: " << ex.what() << endl;
			cout << "trying to reconnect" << endl;
			mongoDbConnect(this->address, this->port);
		}
		catch(...)
		{
			ISFAILED = true;
			cout << "UnKnown: " << endl;
			cout << "trying to reconnect" << endl;
			mongoDbConnect(this->address, this->port);

		}
	} while(ISFAILED);



	this->poolPtr->done(); 

	saveDataState(database, collection, instanceId, customerId, startTime, endTime);

	//delete byteArrays
	for (int i=0;i<byteArrays.size();i++) {
		delete [] byteArrays[i];
	}
	
	delete this->poolPtr;
}

long long MongoC::getLatestDate(string database, string collection, string instanceId, long long customerId) {
	string dbAndColl = database + "_instance_state" + "." + collection;
	BSONObj searchQuery;
	searchQuery = BSONObjBuilder().append(INSTANCE_ID, instanceId).append(CUSTOMER_ID, customerId).obj();
	BSONObj obj = conn->findOne(dbAndColl, searchQuery);
	
	BSONElement elem;
	long long latest;
	
	elem = obj.getField(END_TIME);
	if (elem.eoo()) //field not found
		latest = -1;
	else 
		latest = elem.Long();
		
	this->poolPtr->done(); 
	return latest;
}

void MongoC::saveDataState(string database, string collection, string instanceId, long long customerId, long long startTime, long long endTime) {
	string dbAndColl = database + "_instance_state" + "." + collection;
	BSONObj searchQuery;
	searchQuery = BSONObjBuilder().append(INSTANCE_ID, instanceId).obj();
	BSONObj obj = conn->findOne(dbAndColl, searchQuery);

	if(!obj.isEmpty()) {
		BSONElement elem;
		long long oldEt;
		long long oldSt;
		long long oldArchivedTo;
		long long oldMostRecentlyProcessed;
		long long cpuInfoExists;
		long long isMicro;
		long long assetId;

		elem = obj.getField(END_TIME);
		if (elem.eoo()) //field not found
			oldEt = -1;
		else 
			oldEt = elem.Long();

		elem = obj.getField(START_TIME);
		if (elem.eoo()) //field not found
			oldSt = -1;
		else 
			oldSt = elem.Long();

		elem = obj.getField(DATE_ARCHIVED_TO);
		if (elem.eoo()) //field not found
			oldArchivedTo = -2;
		else 
			oldArchivedTo = elem.Long();

		elem = obj.getField(TIME_OF_MOST_RECENTLY_PROCESSED);
		if (elem.eoo()) //field not found
			oldMostRecentlyProcessed = -2;
		else 
			oldMostRecentlyProcessed = elem.Long();

		elem = obj.getField(SYSTEM_CPU_INFO_EXISTS);
		if (elem.eoo()) //field not found
			cpuInfoExists = -2;
		else 
			cpuInfoExists = elem.Long();

		elem = obj.getField(IS_MICRO);
		if (elem.eoo()) //field not found
			isMicro = -2;
		else 
			isMicro = elem.Long();

		elem = obj.getField(ASSET_ID);
		if (elem.eoo()) //field not found
			assetId = -2;
		else 
			assetId = elem.Long();


		BSONObjBuilder buildNewDocument;	
		if (true || oldEt < endTime || oldEt==-1) { //update in db
			buildNewDocument.append(END_TIME, endTime);			
		}
		if (oldSt > startTime || oldSt==-1) { //update in db
			buildNewDocument.append(START_TIME, startTime);				
		}
		if (oldArchivedTo==-2) {
			oldArchivedTo = -1;
			buildNewDocument.append(DATE_ARCHIVED_TO, oldArchivedTo);		
		}
		if (oldMostRecentlyProcessed==-2) {
			oldMostRecentlyProcessed = -1;
			buildNewDocument.append(TIME_OF_MOST_RECENTLY_PROCESSED, oldMostRecentlyProcessed);		
		}
		if (cpuInfoExists==-2) {
			cpuInfoExists = -1;
			buildNewDocument.append(SYSTEM_CPU_INFO_EXISTS, cpuInfoExists);		
		}
		if (isMicro==-2) {
			isMicro = -1;
			buildNewDocument.append(IS_MICRO, isMicro);		
		}
		if (assetId==-2) {
			assetId = -1;
			buildNewDocument.append(ASSET_ID, assetId);		
		}

		if (true) {
			buildNewDocument.append(CUSTOMER_ID, customerId);
			BSONObj updateObj = BSONObjBuilder().append("$set", buildNewDocument.obj()).obj();


			bool ISFAILED;
			do {
				try {
					conn->update(dbAndColl, searchQuery, updateObj);
					ISFAILED = false;
				}
				catch(mongo::UserException& ue)
				{
					ISFAILED = true;
					cout << "UserException: " << ue.toString() << endl;
					cout << "trying to reconnect" << endl;
					mongoDbConnect(this->address, this->port);
				}
				catch(mongo::MsgAssertionException& ex)
				{
					ISFAILED = true;
					cout << "MsgAssertionException: " << ex.toString() << endl;
					cout << "trying to reconnect" << endl;
					mongoDbConnect(this->address, this->port);
				}
				catch(mongo::DBException& ex)
				{
					ISFAILED = true;
					cout << "DBException: " << ex.toString() << endl;
					cout << "trying to reconnect" << endl;
					mongoDbConnect(this->address, this->port);
				}
				catch(std::exception& ex)
				{
					ISFAILED = true;
					cout << "std::exception: " << ex.what() << endl;
					cout << "trying to reconnect" << endl;
					mongoDbConnect(this->address, this->port);
				}
				catch(...)
				{
					ISFAILED = true;
					cout << "whatever exception, update state failed: " << endl;
					cout << "trying to reconnect" << endl;
					mongoDbConnect(this->address, this->port);
				}
			} while(ISFAILED);



			if (true || oldEt < endTime || oldEt==-1) {
				searchQuery = BSONObjBuilder().append(INSTANCE_ID, instanceId)
					.append(CUSTOMER_ID, customerId)
					.append(TIME_OF_MOST_RECENTLY_PROCESSED, BSONObjBuilder().append("$lte", endTime).obj())//check this, seems ok
					.obj();

				BSONObjBuilder document;
				document.append(READY_TO_PROCESS, true);
				updateObj = BSONObjBuilder().append("$set", document.obj()).obj();// change this



				bool ISFAILED;
				do {
					try {
						conn->update(dbAndColl, searchQuery, updateObj);
						ISFAILED = false;
					}
					catch(mongo::UserException& ue)
					{
						ISFAILED = true;
						cout << "UserException: " << ue.toString() << endl;
						cout << "trying to reconnect" << endl;
						mongoDbConnect(this->address, this->port);
					}
					catch(mongo::MsgAssertionException& ex)
					{
						ISFAILED = true;
						cout << "MsgAssertionException: " << ex.toString() << endl;
						cout << "trying to reconnect" << endl;
						mongoDbConnect(this->address, this->port);
					}
					catch(mongo::DBException& ex)
					{
						ISFAILED = true;
						cout << "DBException: " << ex.toString() << endl;
						cout << "trying to reconnect" << endl;
						mongoDbConnect(this->address, this->port);
					}
					catch(std::exception& ex)
					{
						ISFAILED = true;
						cout << "std::exception: " << ex.what() << endl;
						cout << "trying to reconnect" << endl;
						mongoDbConnect(this->address, this->port);
					}
					catch(...)
					{
						ISFAILED = true;
						cout << "whatever exception, update state failed: "<< endl;
						cout << "trying to reconnect" << endl;
						mongoDbConnect(this->address, this->port);
					}
					
				} while(ISFAILED);


				this->poolPtr->done(); 
			}
		}
	} 
	else 
	{
		BSONObjBuilder document;
		document.append(INSTANCE_ID, instanceId);
		document.append(END_TIME, endTime);
		document.append(START_TIME, startTime);
		document.append(CUSTOMER_ID, customerId);
		document.append(TIME_OF_MOST_RECENTLY_PROCESSED, (long long)-1);
		document.append(DATE_ARCHIVED_TO, (long long)-1);
		document.append(READY_TO_PROCESS, true);
		document.append(SYSTEM_CPU_INFO_EXISTS, (long long)-1);
		document.append(IS_MICRO, (long long)-1);
		document.append(ASSET_ID, (long long)-1);


		bool ISFAILED;
		do {
			try {
				conn->insert(dbAndColl, document.obj());
				ISFAILED = false;
			}
			catch(mongo::UserException& ue)
			{
				ISFAILED = true;
				cout << "UserException: " << ue.toString() << endl;
				cout << "trying to reconnect" << endl;
				mongoDbConnect(this->address, this->port);
			}
			catch(mongo::MsgAssertionException& ex)
			{
				ISFAILED = true;
				cout << "MsgAssertionException: " << ex.toString() << endl;
				cout << "trying to reconnect" << endl;
				mongoDbConnect(this->address, this->port);
			}
			catch(mongo::DBException& ex)
			{
				ISFAILED = true;
				cout << "DBException: " << ex.toString() << endl;
				cout << "trying to reconnect" << endl;
				mongoDbConnect(this->address, this->port);
			}
			catch(std::exception& ex)
			{
				ISFAILED = true;
				cout << "std::exception: " << ex.what() << endl;
				cout << "trying to reconnect" << endl;
				mongoDbConnect(this->address, this->port);
			}
			catch(...)
			{
				ISFAILED = true;
				cout << "exception, insert instance state failed: " << endl;
				cout << "trying to reconnect" << endl;
				mongoDbConnect(this->address, this->port);
			}
			
		} while(ISFAILED);

		this->poolPtr->done();

	}
}




