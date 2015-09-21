package com.udel.mywbob;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.TimeZone;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

public class StoreInstanceState {

	public static Date parsingStringtoDate(String s) {
		DateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss-SSS");
		String input = s;
		Date t =null;
		TimeZone tz = TimeZone.getTimeZone("GMT");
		ft.setTimeZone(tz);
		try {
			t = ft.parse(input);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// parse string to date;
		return t;	
	}
	
	String INSTANCE_ID = "instance_id";
	String CUSTOMER_ID = "customer_id";
	String END_TIME = "end_time";
	String START_TIME = "start_time";
	String READY_TO_PROCESS = "ready_to_process";
	String TIME_OF_MOST_RECENTLY_PROCESSED = "most_recently_processed";
	String DATE_ARCHIVED_TO = "archived_to"; 
	String SYSTEM_CPU_INFO_EXISTS = "system_cpu_info_exists";
	String IS_MICRO = "is_micro";
	String ASSET_ID = "asset_id";
	
	private String address;
	private int port;
	private String database;
	private String collection;
	private String instanceId;
	private long endTime;
	private long startTime;
	private long mostRecentlyProcesses;
	private long customerId;
	private long value;
	private MongoClient connectionInfos;//this has an internal pool for the DBs on a single host
	private static Hashtable<String, MongoClient> hashTable = new Hashtable<String, MongoClient>();//each mongoclient for a single host
	
	
	
	public StoreInstanceState(String addr, int port) {
		this.address = addr;
		this.port = port;	
		mongoDbConnect(addr,port);//check and connect		
	}
	
	
	public void mongoDbConnect(String addr, int port){//this can not keep connections for multi hosts, maybe need to make many instances
		String connectionPointString = new String();
		connectionPointString = "/" + addr + ":" + port;

		if (hashTable.containsKey(connectionPointString)) {//connection is there
			this.connectionInfos = hashTable.get(connectionPointString);
			System.out.println("the connection point is " + connectionInfos.getConnectPoint());//   /128.4.30.3:29000, with a "/"
		}

		else {//connection not found
			System.out.println("new connection");		
			try {//set the autoconnect and other options, then connect
				MongoClientOptions options = new MongoClientOptions.Builder().autoConnectRetry(true).connectTimeout(0).maxAutoConnectRetryTime(1000000).socketKeepAlive(true).socketTimeout(0).build();
				this.connectionInfos = new MongoClient(new ServerAddress(this.address, this.port), options);

			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MongoException me) {
				me.printStackTrace();
			}
			//add into hashtable
			hashTable.put(connectionPointString, this.connectionInfos);
		}	
	}
	
	
	public boolean updateProcessedTo(String db, String coll, String id, long cid, long val) {// update in db
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		this.value = val;
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, instanceId);
		searchQuery.put(CUSTOMER_ID, customerId);
		BasicDBObject document = new BasicDBObject();
		document.put(TIME_OF_MOST_RECENTLY_PROCESSED, value);		
		BasicDBObject updateObj =  new BasicDBObject();
		updateObj.put("$set", document);
		try {
			connectionInfos.getDB(this.database).getCollection(this.collection).update(searchQuery, updateObj);	
			searchQuery.put(END_TIME, BasicDBObjectBuilder.start("$lte", value).get());
			document = new BasicDBObject();
			document.put(READY_TO_PROCESS, false);		
			updateObj =  new BasicDBObject();
			updateObj.put("$set", document);
			connectionInfos.getDB(this.database).getCollection(this.collection).findAndModify(searchQuery, updateObj);
		} catch (MongoException e) {
			return false;
		}
		return true;
		
	}
	
	public String getDoc(BasicDBObject obj) {
		return obj.getString(INSTANCE_ID,"unknown_id") + "_" 
				+ obj.getLong(CUSTOMER_ID,-2) + "_" 
				+ obj.getLong(TIME_OF_MOST_RECENTLY_PROCESSED,-1) + "_"
				+ obj.getLong(START_TIME, -1) + "_"
				+ obj.getLong(END_TIME, -1) + "_"					
				+ obj.getLong(DATE_ARCHIVED_TO,-1) + "_"
				+ obj.getLong(SYSTEM_CPU_INFO_EXISTS,-2) + "_"
				+ obj.getLong(IS_MICRO,-2)+ "_"
				+ obj.getLong(ASSET_ID,-2);
	}

	public String getState(String db, String coll, String id, long cid) {
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		BasicDBObject searchQuery = new BasicDBObject(INSTANCE_ID, instanceId);
		if (customerId!=0)
			searchQuery.put(CUSTOMER_ID,customerId);
		BasicDBObject obj = (BasicDBObject) connectionInfos.getDB(this.database).getCollection(this.collection).findOne(searchQuery);
		String res = getDoc(obj);		
		return res;		
	}	
	
	public ArrayList<String> setStateForQuery(BasicDBObject searchQuery) {
		ArrayList<String> res = new ArrayList<String>();
		DBCursor cursor = connectionInfos.getDB(this.database).getCollection(this.collection).find(searchQuery);
		while (cursor.hasNext()) {
			BasicDBObject obj = (BasicDBObject) cursor.next();
			res.add(getDoc(obj));
		}	
		return res;
	}
	
	public ArrayList<String> findNewThings(String db, String coll) {
		this.database = db;
		this.collection = coll;
		this.customerId = 0;
		return findNewThingsForCustomer(db, coll, this.customerId);
	}	
	
	public ArrayList<String> findNewThingsForCustomer(String db, String coll, long cid) {
		this.database = db;
		this.collection = coll;
		this.customerId = cid;
		ArrayList<String> res = new ArrayList<String>();
		BasicDBObject searchQuery = (BasicDBObject) new BasicDBObject(SYSTEM_CPU_INFO_EXISTS, new BasicDBObject("$ne", (long)0));
		searchQuery.put(READY_TO_PROCESS, true);
		searchQuery.put(IS_MICRO, new BasicDBObject("$ne", (long)1));		
		if (customerId!=0)
			searchQuery.put("customer_id", customerId);
		return setStateForQuery(searchQuery);		
	}

	public ArrayList<String> findRecentEntriesWithMissingCpuInfo(String db, String coll, long oldTime, long cid) {
		this.database = db;
		this.collection = coll;
		this.customerId = cid;
		ArrayList<String> res = new ArrayList<String>();		
		BasicDBObject querySt = new BasicDBObject("end_time", new BasicDBObject("$gt", oldTime));
		querySt.put(SYSTEM_CPU_INFO_EXISTS, 0);
		if (customerId!=0)
			querySt.put("customer_id", customerId);
		return setStateForQuery(querySt);		
	}
	

	public ArrayList<String> findRecentEntriesWithIsMicro(String db, String coll, long oldTime, long cid) {
		this.database = db;
		this.collection = coll;
		this.customerId = cid;
		ArrayList<String> res = new ArrayList<String>();		
		BasicDBObject querySt = new BasicDBObject("end_time", new BasicDBObject("$gt", oldTime));
		querySt.put(IS_MICRO, 1);
		if (customerId!=0)
			querySt.put("customer_id", customerId);
		return setStateForQuery(querySt);
	}

	
	public void saveDataState(String db, String coll, String id, long cid, long st, long et) {// update in db
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		this.startTime = st;
		this.endTime = et;
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, instanceId);		
		DBObject obj = connectionInfos.getDB(this.database).getCollection(this.collection).findOne(searchQuery);		

		if (obj != null) {//update the endTime if new endTime is later than the previous one, for a id		
			long oldEt = ((BasicDBObject) obj).getLong(END_TIME,-1);
			long oldSt = ((BasicDBObject) obj).getLong(START_TIME,-1);
			long oldArchivedTo = ((BasicDBObject) obj).getLong(DATE_ARCHIVED_TO,-2);
			long oldMostRecentlyProcessed = ((BasicDBObject) obj).getLong(TIME_OF_MOST_RECENTLY_PROCESSED,-2);
			long cpuInfoExists = ((BasicDBObject) obj).getLong(SYSTEM_CPU_INFO_EXISTS,-2);
			long isMicro = ((BasicDBObject) obj).getLong(IS_MICRO,-2);
			long assetId = ((BasicDBObject) obj).getLong(ASSET_ID,-2);
			
			BasicDBObject newDocument = null;
			if (true || oldEt < endTime || oldEt==-1) { //update in db
				if (newDocument==null)
					newDocument = new BasicDBObject();
				newDocument.put(END_TIME, endTime);				
			}
			if (oldSt > startTime || oldSt==-1) { //update in db
				if (newDocument==null)
					newDocument = new BasicDBObject();
				newDocument.put(START_TIME, startTime);				
			}
			if (oldArchivedTo==-2) {
				if (newDocument==null)
					newDocument = new BasicDBObject();
				oldArchivedTo = -1;
				newDocument.put(DATE_ARCHIVED_TO, oldArchivedTo);		
			}
			if (oldMostRecentlyProcessed==-2) {
				if (newDocument==null)
					newDocument = new BasicDBObject();
				oldMostRecentlyProcessed = -1;
				newDocument.put(TIME_OF_MOST_RECENTLY_PROCESSED, oldMostRecentlyProcessed);		
			}
			if (cpuInfoExists==-2) {
				if (newDocument==null)
					newDocument = new BasicDBObject();
				cpuInfoExists = -1;
				newDocument.put(SYSTEM_CPU_INFO_EXISTS, cpuInfoExists);		
			}
			if (isMicro==-2) {
				if (newDocument==null)
					newDocument = new BasicDBObject();
				isMicro = -1;
				newDocument.put(IS_MICRO, isMicro);		
			}
			if (assetId==-2) {
				if (newDocument==null)
					newDocument = new BasicDBObject();
				assetId = -1;
				newDocument.put(ASSET_ID, assetId);		
			}
			
			if (newDocument!=null) {
				newDocument.put("customer_id", customerId);
				BasicDBObject updateObj = new BasicDBObject();
				updateObj.put("$set", newDocument);
				
				// update mongoDB
				WriteResult writeResult = null;
				do {//try to reconnect if update failed
					try {
						writeResult = connectionInfos.getDB(this.database).getCollection(this.collection).update(searchQuery, updateObj);
					}
					catch (Exception e){
						e.printStackTrace();
						System.out.println("trying to reconnect...");
						try {//program wait 5 secs, hope the auto reconnect works
							Thread.sleep(5000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
				} while (writeResult == null || writeResult.getError() != null);
				
				
				if (true || oldEt < endTime || oldEt==-1) {
					searchQuery = new BasicDBObject();
					searchQuery.put(INSTANCE_ID, instanceId);
					searchQuery.put(CUSTOMER_ID, customerId);
					searchQuery.put(TIME_OF_MOST_RECENTLY_PROCESSED, BasicDBObjectBuilder.start("$lte", endTime).get());
					BasicDBObject document = new BasicDBObject();
					document.put(READY_TO_PROCESS, true);		
					updateObj =  new BasicDBObject();
					updateObj.put("$set", document);
					
					// findAndModify to mongoDB
					boolean success = true;
					do {//try to reconnect if findAndModify failed
						try {
							connectionInfos.getDB(this.database).getCollection(this.collection).findAndModify(searchQuery, updateObj);
							success = true;
						}
						catch (Exception e){
							e.printStackTrace();
							System.out.println("trying to reconnect...");
							success = false;
							try {//program wait 5 secs, hope the auto reconnect works
								Thread.sleep(5000);
							} catch (InterruptedException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
					} while (!success);
				}
			}			
		} else {//new instaceid, store it			
			BasicDBObject document = new BasicDBObject();
			document.put(INSTANCE_ID, instanceId);
			document.put(END_TIME, endTime);
			document.put(START_TIME, startTime);
			document.put(CUSTOMER_ID, customerId);
			document.put(TIME_OF_MOST_RECENTLY_PROCESSED, -1);
			document.put(DATE_ARCHIVED_TO, -1);
			document.put(READY_TO_PROCESS, true);
			document.put(SYSTEM_CPU_INFO_EXISTS, -1);
			document.put(IS_MICRO, -1);
			document.put(ASSET_ID, -1);
					
			// insert to mongoDB
			WriteResult writeResult = null;
			do {//try to reconnect if insert failed
				try {
					writeResult = connectionInfos.getDB(this.database).getCollection(this.collection).insert(document);
				}
				catch (Exception e){
					e.printStackTrace();
					System.out.println("trying to reconnect...");
					try {//program wait 5 secs, hope the auto reconnect works
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			} while (writeResult == null || writeResult.getError() != null);
			
		}		
	}
	
	
	
	public void setCpuInfoIsAvailable(String db, String coll, String id, long cid, long val) {
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		this.value = val;
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, instanceId);
		searchQuery.put(CUSTOMER_ID, customerId);		
		long cnt = connectionInfos.getDB(this.database).getCollection(this.collection).getCount(searchQuery);
		if (cnt>0) {
			BasicDBObject doc = new BasicDBObject("$set", new BasicDBObject(SYSTEM_CPU_INFO_EXISTS, value));		
			connectionInfos.getDB(this.database).getCollection(this.collection).update(searchQuery, doc);
		}		
	}
	public void setIsMicro(String db, String coll, String id, long cid, long val) {	
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		this.value = val;
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, instanceId);
		searchQuery.put(CUSTOMER_ID, customerId);		
		long cnt = connectionInfos.getDB(this.database).getCollection(this.collection).getCount(searchQuery);
		//System.out.println("fd "+cnt+" instance "+instanceId+" customer: "+customerId);
		if (cnt>0) {
			BasicDBObject doc = new BasicDBObject("$set", new BasicDBObject(IS_MICRO, value));		
			connectionInfos.getDB(this.database).getCollection(this.collection).update(searchQuery, doc);
		}		
	}
	public void setAssetId(String db, String coll, String id, long cid, long val) {	
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		this.value = val;
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, instanceId);
		searchQuery.put(CUSTOMER_ID, customerId);		
		long cnt = connectionInfos.getDB(this.database).getCollection(this.collection).getCount(searchQuery);
		//System.out.println("fd "+cnt+" instance "+instanceId+" customer: "+customerId);
		if (cnt>0) {
			BasicDBObject doc = new BasicDBObject("$set", new BasicDBObject(ASSET_ID, value));		
			connectionInfos.getDB(this.database).getCollection(this.collection).update(searchQuery, doc);
		}		
	}
	
	public long loadLatestDate(String db, String coll, String id, long cid) {
		this.database = db;
		this.collection = coll;
		this.instanceId = id;
		this.customerId = cid;
		long date = 0;
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(INSTANCE_ID, this.instanceId);
		if (customerId!=0)
			searchQuery.put("customer_id",customerId);
					
		DBObject obj = connectionInfos.getDB(this.database).getCollection(this.collection).findOne(searchQuery);

		if (obj == null) {
			date = -1;
		}
		else {
			date = ((BasicDBObject) obj).getLong(END_TIME);
		}
		
		return date;
	}
	
}

