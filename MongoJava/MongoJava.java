

package com.udel.mywbob;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.bson.types.Binary;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;





public class MongoJava {

	public static Date parsingStringtoDate(String s) {
		DateFormat ft = new SimpleDateFormat ("yyyy-MM-dd_HH-mm-ss-SSS");
		String input = s;
		Date t =null;
		TimeZone tz = TimeZone.getTimeZone("GMT"); //use this so the sec is correct
		ft.setTimeZone(tz);
		try {
			t = ft.parse(input);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// parse string to date;
		return t;	
	}
	
	//reduce dup code
	public static byte[] toByteArray(float value) {//float to byte array
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putFloat(value);
		return bytes;
	}

	public static byte[] toByteArray(double value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(value);
		return bytes;
	}

	public static byte[] toByteArray(int value) {
		byte[] bytes = new byte[4];
		ByteBuffer.wrap(bytes).putInt(value);
		return bytes;
	}

	public static byte[] toByteArray(short value) {
		byte[] bytes = new byte[2];
		ByteBuffer.wrap(bytes).putShort(value);
		return bytes;
	}

	public static byte[] toByteArray(long value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putLong(value);
		return bytes;
	}


	public static byte[] concat(byte[] dataa, byte[] datab) {//concat two byte arrays
		byte[] result = Arrays.copyOf(dataa, dataa.length + datab.length);
		System.arraycopy(datab, 0, result, dataa.length, datab.length);
		return result;
	}


	public static float[] concat(float[] dataa, float[] datab) {//concat two float arrays
		float[] result = Arrays.copyOf(dataa, dataa.length + datab.length);
		System.arraycopy(datab, 0, result, dataa.length, datab.length);
		return result;
	}

	public static int[] concat(int[] dataa, int[] datab) {//concat two int arrays
		int[] result = Arrays.copyOf(dataa, dataa.length + datab.length);
		System.arraycopy(datab, 0, result, dataa.length, datab.length);
		return result;
	}

	public static long[] concat(long[] dataa, long[] datab) {//concat two long arrays
		long[] result = Arrays.copyOf(dataa, dataa.length + datab.length);
		System.arraycopy(datab, 0, result, dataa.length, datab.length);
		return result;
	}

	public static double[] concat(double[] dataa, double[] datab) {//concat two double arrays
		double[] result = Arrays.copyOf(dataa, dataa.length + datab.length);
		System.arraycopy(datab, 0, result, dataa.length, datab.length);
		return result;
	}

	private String address;
	private int port;
	private String database;
	private String collection;
	private int numOfRows;
	private int numOfCols;
	private String dataPattern;
	private BasicDBObject metaDataObj;
	private long lastDate;
	private String instanceId;
	private long startTime;
	private long endTime;
	private long customerId;
	private boolean SHOW = false;
	private MongoClient connectionInfos;//this has an internal pool for the DBs on a single host
	private static Hashtable<String, MongoClient> hashTable = new Hashtable<String, MongoClient>();//each mongoclient for a single host


	//only one constructor for connecting
	public MongoJava(String addr, int port) {
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

	public long mongoDbDateToLong (String fromDate) {
		//use /1000 to convert millisec to sec
		return parsingStringtoDate(fromDate).getTime() / 1000;
	}

	public long mongoDbGetLastDate () {
		return this.lastDate;
	}

	public void mongoDbSetLastDate (long ld) {
		this.lastDate = ld;
	}

	public List<String> getInstances(int cid) {		
		this.customerId = cid;
		BasicDBObject q = new BasicDBObject();
		q.put("customer_id",customerId);
		//q.put("metadata.instance_id", instanceId);
		List<String> res = connectionInfos.getDB(this.database).getCollection(this.collection).distinct("instance_id",q);
		return res;		
	}

	public List<Long> getCustomers() {		
		BasicDBObject q = new BasicDBObject();
		List<Long> res = connectionInfos.getDB(this.database).getCollection(this.collection).distinct("customer_id");
		return res;		
	}

	public List<Long> getEndTimes(int cid, String id) {		
		this.customerId = cid;
		this.instanceId = id;
		BasicDBObject q = new BasicDBObject();
		q.put("customer_id",customerId);
		q.put("instance_id", instanceId);
		List<Long> res = connectionInfos.getDB(this.database).getCollection(this.collection).distinct("end_time",q);
		return res;		
	}

	public List<Long> getStartTimes(int cid, String id) {	
		this.customerId = cid;
		this.instanceId = id;
		BasicDBObject q = new BasicDBObject();
		q.put("customer_id",customerId);
		q.put("instance_id", instanceId);
		List<Long> res = connectionInfos.getDB(this.database).getCollection(this.collection).distinct("start_time",q);
		return res;		
	}

	public ArrayList<Long> getTimes(int cid, String id) {
		this.customerId = cid;
		this.instanceId = id;
		ArrayList<Long> res = new ArrayList<Long>();
		DBCursor dataForOutputList;
		BasicDBObject q = new BasicDBObject();
		q.put("customer_id",customerId);
		q.put("instance_id", instanceId);		
		dataForOutputList = connectionInfos.getDB(this.database).getCollection(this.collection).find(q);
		while (dataForOutputList.hasNext()) {// iterate all the data that fulfill the queryOr, maybe can break it at some point to limit the data
			//startTimer = System.nanoTime();
			DBObject f = dataForOutputList.next();

			Long startTime = (Long) f.get("start_time");
			Long endTime = (Long) f.get("end_time");
			res.add(startTime);
			res.add(endTime);
		}
		return res;
	}



	public ArrayList<TheData> mongoDbLoadObjGzip(String db, String coll, String id, long st, long et, int[] colsToGet) {// load files in range [startDateToGet, endDateToGet]

		//result list
		ArrayList<TheData> res = new ArrayList<TheData>();


		boolean reTry = false;
		do {//protect the load, if any exception raise, just sleep and redo, hope the auto reconnect works
			try {
				//maybe use set method is better for encapsulation
				this.database = db;
				this.collection = coll;
				this.instanceId = id;
				this.startTime = st;
				this.endTime = et;

				String instanceIdToGet = instanceId;
				long startTimeToGet = startTime;
				long endTimeToGet = endTime;


				if (true)//SHOW)
					System.out.println("instance: "+instanceId+" start time: "+startTimeToGet+" end time: "+ endTimeToGet);

				// start_time<st && end_time>st 
				// || start_time==st
				// || start_time>st && start_time<et

				//query
				DBCursor dataForOutputList;
				if (endTimeToGet!=0) {	
					dataForOutputList = connectionInfos.getDB(this.database).getCollection(this.collection).find(	
							new QueryBuilder().or(
									new QueryBuilder().put("start_time").lessThan(startTimeToGet).put("end_time").greaterThan(startTimeToGet).get(),
									new QueryBuilder().put("start_time").is(startTimeToGet).get(),
									new QueryBuilder().put("start_time").greaterThan(startTimeToGet).put("start_time").lessThan(endTimeToGet).get()
									).put("instance_id").is(instanceIdToGet).get()
							);
				} else {
					dataForOutputList = connectionInfos.getDB(this.database).getCollection(this.collection).find(				
							new QueryBuilder().or(
									new QueryBuilder().put("start_time").lessThan(startTimeToGet).put("end_time").greaterThan(startTimeToGet).get(),							
									new QueryBuilder().put("start_time").greaterThan(startTimeToGet).get()
									).put("instance_id").is(instanceIdToGet).get()
							);
				}

				//iter all the data
				while (dataForOutputList.hasNext()) {// iterate all the data that fulfill the queryOr, maybe can break it at some point to limit the data

					DBObject f = dataForOutputList.next();
					String dp = (String) f.get("data_pattern");//get the data pattern from database


					if (true)
						System.out.println("got "+ f.get("instance_id") + " start " + f.get("start_time") + " end " + f.get("end_time") );


					StringBuilder resDp = new StringBuilder();//data pattern
					StringBuilder sb = new StringBuilder();//merge all the num to string. etc 1.1 2.2 3.3 4.4. 
					ArrayList <String> ss = new ArrayList <String>();// all the strings, convert to sting[] later

					for (int l=0;l<dp.length();l++) {
						if (l < colsToGet.length && colsToGet[l] == 1) {//only get the cols I want
							InputStream in = new ByteArrayInputStream((byte[]) f.get("binData" + l));
							resDp.append(dp.charAt(l));


							if (dp.charAt(l)== 'i') {//load int
								GZIPInputStream gis =null;
								try {
									gis = new GZIPInputStream(in);
									byte[] tempbytes = new byte[4];//int 4 bytes
									while (gis.read(tempbytes, 0 , 4 ) != -1) {//read 4 bytes until the end of stream
										int t = ByteBuffer.wrap(tempbytes).getInt();
										sb.append(t);
										sb.append(" ");
									}
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							else if (dp.charAt(l)== 'l') {//load long
								GZIPInputStream gis =null;
								try {
									gis = new GZIPInputStream(in);
									byte[] tempbytes = new byte[8];//long 8 bytes
									while (gis.read(tempbytes, 0 , 8 ) != -1) {//read 8 bytes until the end of stream
										long t = ByteBuffer.wrap(tempbytes).getLong();
										sb.append(t);
										sb.append(" ");
									}
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							else if (dp.charAt(l)== 'd') {//load double
								GZIPInputStream gis =null;
								try {
									gis = new GZIPInputStream(in);
									byte[] tempbytes = new byte[8];// double 8 bytes
									while (gis.read(tempbytes, 0 , 8 ) != -1) {//read 8 bytes until the end of stream
										double t = ByteBuffer.wrap(tempbytes).getDouble();
										sb.append(t);
										sb.append(" ");
									}
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							else if (dp.charAt(l)== 'f') {//load float
								GZIPInputStream gis =null;
								try {
									gis = new GZIPInputStream(in);
									byte[] tempbytes = new byte[4];//float 4 bytes
									while (gis.read(tempbytes, 0 , 4 ) != -1) {//read 4 bytes until the end of stream
										float t = ByteBuffer.wrap(tempbytes).getFloat();
										sb.append(t);
										sb.append(" ");
									}					
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							else if (dp.charAt(l)== 's') {//short string, 2 bytes for string len
								GZIPInputStream gis = null;
								try {
									gis = new GZIPInputStream(in);
									while (true) {//read until end of stream
										byte[] strlen = new byte[2];// two byte for the length of the short string
										if (gis.read(strlen, 0 , strlen.length) == -1) break;//read the string len
										//System.out.println(ByteBuffer.wrap(strlen).getShort());
										byte[] tempdata = new byte[ByteBuffer.wrap(strlen).getShort()];//string len byte array to short
										gis.read(tempdata, 0 , tempdata.length);//read the string
										String stemp = new String(tempdata);
										ss.add(stemp);
									}
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
							else {//text , 4 bytes for string len
								GZIPInputStream gis = null;
								try {
									gis = new GZIPInputStream(in);
									while (true) {//read until end of stream
										byte[] strlen = new byte[4];// 4 byte for short string
										if (gis.read(strlen, 0 , strlen.length) == -1) break;//read the string len
										//System.out.println(ByteBuffer.wrap(strlen).getInt());
										byte[] tempdata = new byte[ByteBuffer.wrap(strlen).getInt()];//string len byte array to int
										gis.read(tempdata, 0 , tempdata.length);//read the string
										String stemp = new String(tempdata);
										ss.add(stemp);
									}
									gis.close();
								} catch (IOException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
						}
					}

					String[] sArray = ss.toArray(new String[ss.size()]);	
					res.add(new TheData(sb.toString(),sArray,resDp.toString()));
				} 

				reTry = false;

			} catch (Exception e) {
				e.printStackTrace();
				//sleep
				System.out.println("sleep 10");
				try {//program wait 5 secs, hope the auto reconnect works
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				reTry = true;
			}
		} while (reTry);

		return res;


	}


	public ArrayList<float []> mongoDbLoadFloatGzip(String db, String coll, String id, long st, long et) {// load files in range [startDateToGet, endDateToGet]

		//result list
		ArrayList<float []> res = null; 
		boolean reTry = false;
		do {//protect the load, if any exception raise, just sleep and redo, hope the auto reconnect works
			try {
				//maybe use set method is better for encapsulation
				this.database = db;
				this.collection = coll;
				this.instanceId = id;
				this.startTime = st;
				this.endTime = et;


				String instanceIdToGet = instanceId;
				long startTimeToGet = startTime;
				long endTimeToGet = endTime;

				res =new ArrayList<float []>();


				if (true)//SHOW)
					System.out.println("instance: "+instanceId+" start time: "+startTimeToGet+" end time: "+ endTimeToGet);

				DBCursor dataForOutputList;
				if (endTimeToGet!=0) {	
					dataForOutputList = connectionInfos.getDB(this.database).getCollection(this.collection).find(	
							new QueryBuilder().or(
									new QueryBuilder().put("start_time").lessThan(startTimeToGet).put("end_time").greaterThan(startTimeToGet).get(),
									new QueryBuilder().put("start_time").is(startTimeToGet).get(),
									new QueryBuilder().put("start_time").greaterThan(startTimeToGet).put("start_time").lessThan(endTimeToGet).get()
									).put("instance_id").is(instanceIdToGet).get()
							);
				} else {
					dataForOutputList = connectionInfos.getDB(this.database).getCollection(this.collection).find(				
							new QueryBuilder().or(
									new QueryBuilder().put("start_time").lessThan(startTimeToGet).put("end_time").greaterThan(startTimeToGet).get(),							
									new QueryBuilder().put("start_time").greaterThan(startTimeToGet).get()
									).put("instance_id").is(instanceIdToGet).get()
							);
				}


				while (dataForOutputList.hasNext()) {// iterate all the data that fulfill the queryOr, maybe can break it at some point to limit the data
					DBObject f = dataForOutputList.next();
					String dp = (String) f.get("data_pattern");
					int col = dp.length();
					ArrayList<Float> outputData = new ArrayList<Float>(); 

					if (true)
						System.out.println("got "+ f.get("instance_id") + " start " + f.get("start_time") + " end " + f.get("end_time") );


					for (int j=0; j< col; j++) {
						InputStream in = new ByteArrayInputStream((byte[]) f.get("binData" + j));

						//unzip the data back
						GZIPInputStream gis =null;
						try {
							gis = new GZIPInputStream(in);
							byte[] tempbytes = new byte[4];
							while (gis.read(tempbytes, 0 , 4 ) != -1) {
								float tempfloat = ByteBuffer.wrap(tempbytes).getFloat();
								outputData.add(tempfloat);
							}
							gis.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

					} 
					//arraylist to float[]
					//float[] floatArr = outputData.toArray(new float[outputData.size()]);	
					float[] floatArr = new float[outputData.size()];
					for (int x=0; x<outputData.size();x++) {
						floatArr[x] = outputData.get(x);
					}


					//add into result arraylist
					res.add(floatArr);

				}
				reTry = false;

			} catch (Exception e) {
				e.printStackTrace();
				//sleep
				System.out.println("sleep 10");
				try {//program wait 5 secs, hope the auto reconnect works
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				reTry = true;
			}
		} while (reTry);

		return res;


	}


	public void mongoDbSaveFloatGzip(String db, String coll, int rows, int cols, String dp, float[] inputNum, String id, long st, long et, int cid) {
		//maybe use set method is better for encapsulation
		this.database = db;
		this.collection = coll;
		this.numOfRows = rows;
		this.numOfCols = cols;
		this.dataPattern = dp;
		this.customerId = cid;
		this.instanceId = id;
		this.startTime = st;
		this.endTime = et;


		this.metaDataObj = new BasicDBObject("instance_id", instanceId).append("start_time", startTime)
				.append("end_time", endTime).append("data_pattern", dataPattern)
				//.append("num_of_rows", numOfRows).append("num_of_cols", numOfCols).append("prefix", preFix)
				.append("customer_id", customerId);

		//System.out.println(dataPattern + " " + dataPattern.length());
		int indexFloat = 0;
		for (int k=0;k<dataPattern.length();k++) {
			int pos = 0;
			byte[] data = new byte[4 * numOfRows];//float 4 byte


			for (int h=0;h<numOfRows;h++){
				byte[] barr = toByteArray(inputNum[indexFloat++]);
				System.arraycopy(barr, 0, data, pos, barr.length);
				pos = pos + 4;
			}


			ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length);
			GZIPOutputStream zipStream = null;
			try {
				zipStream = new GZIPOutputStream(byteStream);
				zipStream.write(data);
				zipStream.close();
				byteStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			byte[] compressedd = byteStream.toByteArray();

			/*
			//print the byte []
			byte[] testTheBinData = compressedd;
			System.out.println("the byte array is");
			for (int i = 0 ; i < testTheBinData.length; i++) {
				System.out.print(testTheBinData[i] & 0xff);
			}
			System.out.println();
			 */ 

			//append each col as binData
			this.metaDataObj.append("binData" + k, new Binary(compressedd));

		}

		// save the binary file into mongoDB
		WriteResult writeResult = null;
		do {//try to reconnect if insert failed, or maybe surround all the code with try catch...
			try {
				writeResult = connectionInfos.getDB(this.database).getCollection(this.collection).insert(metaDataObj);
			}
			catch (Exception e){//all the exception, may not a good idea, but do not know how to catch socket exception
				//System.out.println("last error is " + writeResult.getError());//null exception if try faild
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


	public void mongoDbSaveGzip(String db, String coll, String pf, int rows, int cols, String dp, /*double[] inNum,*/ String[] inStr, String id, long st, long et, int cid) {
		//maybe use set method is better for encapsulation
		this.database = db;
		this.collection = coll;
		this.numOfRows = rows;
		this.numOfCols = cols;
		this.dataPattern = dp;
		this.customerId = cid;
		this.instanceId = id;
		this.startTime = st;
		this.endTime = et;
		this.metaDataObj = new BasicDBObject("instance_id", instanceId).append("start_time", startTime)
				.append("end_time", endTime).append("data_pattern", dataPattern)
				.append("customer_id", customerId);

		System.out.println(dataPattern + " " + dataPattern.length());

		int indexStr = 0;
		for (int k=0;k<dataPattern.length();k++) {
			int pos = 0;

			byte[] data = null;
			if (dataPattern.charAt(k) == 'i') {//int
				data = new byte[4 * numOfRows];
				for (int h=0;h<numOfRows;h++){
					byte[] barr = toByteArray(Integer.parseInt(inStr[indexStr++]));//barr temp array is not necessary
					System.arraycopy(barr, 0, data, pos, barr.length);
					pos = pos + 4;
				}
			}
			else if (dataPattern.charAt(k) == 'd') {//double
				data = new byte[8 * numOfRows];
				for (int h=0;h<numOfRows;h++){
					//System.out.println("darr len " + darr.length);
					byte[] barr = toByteArray(Double.parseDouble(inStr[indexStr++]));
					System.arraycopy(barr, 0, data, pos, barr.length);
					pos = pos + 8;
				}
			}
			else if (dataPattern.charAt(k) == 'l') {//long
				data = new byte[8 * numOfRows];
				for (int h=0;h<numOfRows;h++){
					byte[] barr = toByteArray(Long.parseLong(inStr[indexStr++]));
					System.arraycopy(barr, 0, data, pos, barr.length);
					pos = pos + 8;
				}
			}
			else if (dataPattern.charAt(k) == 'f') {//float
				data = new byte[4 * numOfRows];
				for (int h=0;h<numOfRows;h++){
					byte[] barr = toByteArray(Float.parseFloat(inStr[indexStr++]));
					System.arraycopy(barr, 0, data, pos, barr.length);
					pos = pos + 4;
				}
			}
			else {//string
				int totalBytes = 0;//total bytes for this col
				int tempIndex = indexStr;
				int typeSize;
				int[] stringLens = new int[numOfRows];

				if (dataPattern.charAt(k) == 's') {typeSize = 2;}//for short string, short 2 bytes
				else {typeSize = 4;}//for long string, text ,int 4 bytes

				for (int i=0;i<numOfRows;i++){//convert each string to byte[] for one col
					stringLens[i] = inStr[indexStr].length();
					totalBytes += inStr[indexStr++].length() + typeSize;
				}
				System.out.println("string size for this col " + totalBytes);
				data = new byte[totalBytes];//the data to be compressed
				for (int j=0;j<numOfRows;j++) {
					byte barr[];
					if (typeSize == 2)
						barr = toByteArray((short)stringLens[j]);//2 bytes
					else
						barr = toByteArray(stringLens[j]);//4 bytes
					byte[] strBytes = inStr[tempIndex++].getBytes();
					System.arraycopy(barr, 0, data, pos,barr.length);
					pos = pos + typeSize;
					System.arraycopy(strBytes, 0, data, pos,strBytes.length);
					pos = pos + strBytes.length;

				}


			}



			ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length);
			GZIPOutputStream zipStream = null;
			try {
				zipStream = new GZIPOutputStream(byteStream);
				zipStream.write(data);
				zipStream.close();
				byteStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			byte[] compressedd = byteStream.toByteArray();

			// prepare the data
			//append each col as binData
			this.metaDataObj.append("binData" + k, new Binary(compressedd));

		}

		// save the binary file into mongoDB
		WriteResult writeResult = null;
		do {//try to reconnect if insert failed
			try {
				writeResult = connectionInfos.getDB(this.database).getCollection(this.collection).insert(metaDataObj);
			}
			catch (Exception e){//all the exception, may not a good idea
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


	public static void main(String[] args) { 


	}

}
