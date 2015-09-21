#include "DataMovement.h"

string itos(int i) // convert int to string
{
    stringstream s;
    s << i;
    return s.str();
}

float htonf(float val) {
    uint32_t rep;
    memcpy(&rep, &val, sizeof rep);
    rep = htonl(rep);
    memcpy(&val, &rep, sizeof rep);
    return val;
}

void toByteArray_htonf(float value, Byte * barr) {
	float temp = htonf(value);
	memcpy (barr, &temp, sizeof(value));
}

void toByteArray_htoni(int value, Byte * barr) {
	int temp = htonl(value);
	memcpy (barr, &temp, sizeof(value));
}

void toByteArray_htons(short value, Byte * barr) {
	short temp = htons(value);
	memcpy (barr, &temp, sizeof(value));
}

void toByteArray_htond(double value, Byte * barr) {
	if (htonl((int)123) == ((int)123)) {//	check endianness
			//cout << "big endian" << endl; 
			memcpy (barr, &value, sizeof(value));
	}//big endian no need to change order
	else {
		//cout << "little endian" << endl;
		memcpy (barr, &value, sizeof(value));
		Byte temp;//1, 2, 3, 4, 5, 6, 7, 8
		memcpy (&temp, barr, sizeof(Byte));
		memcpy (barr, barr + 7, sizeof(Byte));
		memcpy (barr + 7, &temp, sizeof(Byte));// swap 1,8
		memcpy (&temp, barr + 1, sizeof(Byte));
		memcpy (barr + 1, barr + 6, sizeof(Byte));
		memcpy (barr + 6, &temp, sizeof(Byte));// swap 2,7
		memcpy (&temp, barr + 2, sizeof(Byte));
		memcpy (barr + 2, barr + 5, sizeof(Byte));
		memcpy (barr + 5, &temp, sizeof(Byte));// swap 3,6
		memcpy (&temp, barr + 3, sizeof(Byte));
		memcpy (barr + 3, barr + 4, sizeof(Byte));
		memcpy (barr + 4, &temp, sizeof(Byte));// swap 4,5
	}
}

void toByteArray_htonll(long long value, Byte * barr) {
	if (htonl((int)123) == ((int)123)) {
		//cout << "big endian" << endl; 
		memcpy (barr, &value, sizeof(value));
	}//big endian no need to change order
	else {
		//cout << "little endian" << endl;
		memcpy (barr, &value, sizeof(value));
		Byte temp;//1, 2, 3, 4, 5, 6, 7, 8
		memcpy (&temp, barr, sizeof(Byte));
		memcpy (barr, barr + 7, sizeof(Byte));
		memcpy (barr + 7, &temp, sizeof(Byte));// swap 1,8
		memcpy (&temp, barr + 1, sizeof(Byte));
		memcpy (barr + 1, barr + 6, sizeof(Byte));
		memcpy (barr + 6, &temp, sizeof(Byte));// swap 2,7
		memcpy (&temp, barr + 2, sizeof(Byte));
		memcpy (barr + 2, barr + 5, sizeof(Byte));
		memcpy (barr + 5, &temp, sizeof(Byte));// swap 3,6
		memcpy (&temp, barr + 3, sizeof(Byte));
		memcpy (barr + 3, barr + 4, sizeof(Byte));
		memcpy (barr + 4, &temp, sizeof(Byte));// swap 4,5
	}
}


//split by delim
std::vector<std::string> tokenize(const std::string & str, const std::string & delim)
{
  vector<string> tokens;
  size_t p0 = 0, p1 = string::npos;//npos is the end of strings 
  while(p0 != string::npos)
  {
    p1 = str.find_first_of(delim, p0);// find first delim at/after p0, not before
    if(p1 != p0)
    {
      string token = str.substr(p0, p1 - p0);
      tokens.push_back(token);
    }
    p0 = str.find_first_not_of(delim, p1);
  }
  return tokens;
}


bool getLineData(const std::string & str, const std::string & delim, vector<string> &linevec, std::string dp) //where to return false to tell an error? s in dp but no double quote may product an error
{
		std::vector<std::string> tempForString;
		std::string numStr = "";
		
		//find the string first, and get the part that is num and concat it to long string
		std::vector<std::string> tokens = tokenize( str, delim );
		if (str[0] == '\"') {//str start with double quote, so the odd chunks are the strings, (but the index is even, 0,2,4....) 
			for (int i=0;i<tokens.size();i++) {
				if (i % 2 == 0) {//the odd chunks
					tempForString.push_back(tokens[i]);
				}
				else {//the even chunks
					numStr = numStr + tokens[i];//concat num to long string and split by " " later
				}
			}
		}
		else {//even chunks are strings
			for (int i=0;i<tokens.size();i++) {
				if (i % 2 == 0) {//the odd chunks
					numStr = numStr + tokens[i];////concat num to long string and split by " " later
				}
				else {//the even chunks
					tempForString.push_back(tokens[i]);
				}
			}
		}

		//split the long string by " "(space), to get a list of number
		std::vector<std::string> tempForNum = tokenize( numStr, " " );
		
		
		//debug printf
		/*
		printf("\nsplit by double quote size %d\n", tokens.size());
		for( int x=0;x<tokens.size();x++)
		{
			std::cout << tokens[x] << std::endl;
		}
		
		printf("\ntempForString size %d\n", tempForString.size());
		for( int x=0;x<tempForString.size();x++)
		{
			std::cout << tempForString[x] << std::endl;
		}
		
		printf("\ntempForNum size %d\n", tempForNum.size());
		for( int x=0;x<tempForNum.size();x++)
		{
			std::cout << tempForNum[x] << std::endl;
		}
		*/
			
		
		//combine tempForString and tempForNum in the right order, same as datapattern...and return it
		int strIndex = 0;
		int numIndex = 0;
		for (int i=0;i<dp.length();i++) {
			//printf("i, str, num  %d %d %d\n", i, strIndex, numIndex);
			if (dp[i] == 's' || dp[i] == 't') {
				linevec.push_back(tempForString[strIndex++]);
			}
			else {//numbers
				linevec.push_back(tempForNum[numIndex++]);
			}
		}

		
		//debug printf
		/*
		printf("\nthe output size %d\n", linevec.size());
		for( int x=0;x<linevec.size();x++)
		{
			std::cout << linevec[x] << std::endl;
		}
		*/
		
		return true;
}

bool readFileOfNumbersAndStrings(string source_file, vector<vector <string> > &dataVec, int &numOfRows, int &numOfCols, string dataPattern) {//after read, numOfRows, numOfCols with right number
	ifstream filein;
	filein.open(source_file.c_str(), ios::in);
	if (!filein.is_open())
	{
		cout << "Error opening file" << endl;
		return false;
	}

	//read each line get the tokens, then push each line to 2d vector according to the datapattern
	//after read the file, the 2d vector holds the data
	//dataVec[i] has the ith col in the file
	string delim = "\"";
	string str;
	int lastCols=0;
	numOfCols = 0, 
	numOfRows = 0;
	//init the first level of the 2d vector 
	dataVec.resize(dataPattern.length());
	
	while(getline(filein, str)) {
		numOfCols = 0;
		vector<string> linevec;
		if(!getLineData(str, delim, linevec, dataPattern)) return false;//read line
		

		for (int i=0;i<dataPattern.length();i++) {
			dataVec[i].push_back(linevec[i]);
			numOfCols++;
		}

		if (lastCols!=0 && lastCols!=numOfCols) {
			cout << "error: there should be the same number of columns on each row"<<endl;
			return false;
		}
		lastCols = numOfCols;
		numOfRows++;
	}
		return true;
}


void twodVec2CompressedByteArrays(vector< vector<string> > &dataVec, int row, int col, string dp, vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays) {//right row col here, use them	
	//DEBUG:val
	int printHelper = 0;
	
	
	//convert the 2d vector data to byte array then compress col by col
	for (int x = 0; x< dp.length(); x++) {
		int pos = 0;
		int len;
		Byte * tempByteArray;//
		if (dp[x] == 'd') {
			len = row * sizeof(double);
			tempByteArray = new Byte[len];
			Byte barr[sizeof(double)];

			for(int i=0; i < row; i++)    //change string in 2d vector, dataVec,  to the right type here
			{			
				toByteArray_htond((double) atof((dataVec[x][i]).c_str()), barr);  //dataVec[a][b], a is the col in org file, b is the row in org file
				memcpy(tempByteArray + pos, barr, sizeof(double));
				pos = pos + sizeof(double); 
			}
		}
		else if (dp[x] == 'i') {
			len = row * sizeof(int);
			tempByteArray = new Byte[len];
			Byte barr[sizeof(int)];

			for(int i=0; i < row; i++)    //change string in 2d vector, dataVec,  to the right type here
			{			
				toByteArray_htoni((int) atoi((dataVec[x][i]).c_str()), barr); 
				memcpy(tempByteArray + pos, barr, sizeof(int));
				pos = pos + sizeof(int); 
			}
		}
		else if (dp[x] == 'l') {
			len = row * sizeof(long long);
			tempByteArray = new Byte[len];
			Byte barr[sizeof(long long)];

			for(int i=0; i < row; i++)    //change string in 2d vector, dataVec,  to the right type here
			{			
				toByteArray_htonll((long long) atoll((dataVec[x][i]).c_str()), barr);
				memcpy(tempByteArray + pos, barr, sizeof(long long));
				pos = pos + sizeof(long long); 
			}
		}
		else if (dp[x] == 'f') {
			len = row * sizeof(float);
			tempByteArray = new Byte[len];
			Byte barr[sizeof(float)];

			for(int i=0; i < row; i++)    //change string in 2d vector, dataVec,  to the right type here
			{	//atof return double so cast to float 		
				toByteArray_htonf((float) atof((dataVec[x][i]).c_str()), barr);
				memcpy(tempByteArray + pos, barr, sizeof(float));
				pos = pos + sizeof(float); 
			}
		}
		else {//string, s for string, t for text
			len = 0;		
			if (dp[x] == 's') {//two bytes for the len
				//cal the len for this col and total col for all the string
				vector<int> stringLens(row);
				Byte barr[sizeof(short)];
				for (int s=0; s<row;s++) {
					stringLens[s] = dataVec[x][s].length();//the length of string should be equal to the length of byte array
					len = len + stringLens[s] + sizeof(short);
				}
				
				tempByteArray = new Byte[len];

				//byte array to be compressed
				for (int i=0; i<row;i++) {
					//!!!!!!!!!!!!change endianness if neceassry, seems I do not need to 
					Byte * strBytes = (Byte *)(dataVec[x][i].data());
					toByteArray_htons((short) stringLens[i], barr);
					memcpy(tempByteArray + pos, barr, sizeof(short));
					pos = pos + sizeof(short);
					memcpy(tempByteArray + pos, strBytes, stringLens[i]);
					pos = pos + stringLens[i];
				}				
			}
			else {//four bytes for the len
				//cal the len for this col and total col for all the string
				vector<int> stringLens(row);
				Byte barr[sizeof(int)];
				for (int s=0; s<row;s++) {
					stringLens[s] = dataVec[x][s].length();//the length of string should be equal to the length of byte array
					len = len + stringLens[s] + sizeof(int);
				}
				
				tempByteArray = new Byte[len];

				//byte array to be compressed
				for (int i=0; i<row;i++) {
					//!!!!!!!!!!!!change endianness if neceassry, seems I do not need to 
					Byte * strBytes = (Byte *)(dataVec[x][i].data());
					toByteArray_htoni((int) stringLens[i], barr);
					memcpy(tempByteArray + pos, barr, sizeof(int));
					pos = pos + sizeof(int);
					memcpy(tempByteArray + pos, strBytes, stringLens[i]);
					pos = pos + stringLens[i];
				}
			}
			
			
		}
		
		
		//prepare to compress
		Byte* byteArray = new Byte[len+20]; // output stored here, delete outside
		int lengthOfCompressedByteArray;
		compressData(tempByteArray, len, byteArray, lengthOfCompressedByteArray);	
		
		
		/*
		//DEBUG:print the org byte[]
		cout << "print the org byte[] " << len << endl;
		for (int cc = 0; cc< len; cc++) {
			printf("%d", tempByteArray[cc] & 0xff);
		}
		cout << endl;
		
		//DEBUG:print the compressed byte[]
		cout << "print the compressed byte[] " << lengthOfCompressedByteArray << endl;
		for (int cc = 0; cc< lengthOfCompressedByteArray; cc++) {
			printf("%d", byteArray[cc] & 0xff);
		}
		cout << endl;
		*/
		
		
		//add this col of compressed data to my buffer
		byteArrays.push_back(byteArray);
		lengthOfCompressedArrays.push_back(lengthOfCompressedByteArray);
		
		
		/*
		//DEBUG:print the compressed byte[] 
		cout << "!!!!!!!!!!!!!compressed byte[] in vector" << endl;
		for (int cc = 0; cc< lengthOfCompressedArrays[printHelper]; cc++) {
			printf("%d", *(byteArrays[printHelper] + cc) & 0xff);
		}
		cout << endl;
		
		//DEBUG:val
		printHelper++;
		*/
		

		delete [] tempByteArray;
	}
}

bool fileOfNumOrString2CompressedByteArrays(string sourceFile, string dp, vector<Byte *> &byteArrays,  vector<int> &lengthOfCompressedArrays) {
	vector<vector <string> > dataVec;
	int numOfRows, numOfCols;
	bool success = readFileOfNumbersAndStrings(sourceFile, dataVec, numOfRows, numOfCols, dp); //may need to get dp info from the filename 	
	if (success) {
		twodVec2CompressedByteArrays(dataVec, numOfRows, numOfCols, dp, byteArrays, lengthOfCompressedArrays);
		return true;
	} else {
		return false;
	}
}


bool compressedDataToFile(vector<Byte *> &byteArrays, vector<int> &lengthOfCompressedArrays, string dp, string destFile) {
	ofstream out;
	out.open(destFile.c_str(), ios::out | ios::binary);
	if (out.is_open())
	{
		cout << "File successfully open" << endl;
	}
	else
	{
		cout << "Error opening file" << endl;
		return false;
	}
	

	int versionNum = 1;
	int dpLen = dp.length();
		
	//write version number
	out.write((char *)&versionNum, sizeof(int));
	//write datapattern len
	out.write((char *)&dpLen, sizeof(int));
	//write datapattern
	out.write(dp.c_str(), dp.length());
				
	int numOfCols = dp.length();
	for (int i=0;i<numOfCols;i++) {//write each col len and col data one by one
		//write len
		out.write((char *)&lengthOfCompressedArrays[i], sizeof(int));
		//write data
		out.write((char *)&(byteArrays[i])[0], lengthOfCompressedArrays[i] * sizeof(Byte));
		
		/*
		//DEBUG:compressed byte []
		cout << "!!!!!!!!!!!!!compressed byte[] in vector, in write to file " << lengthOfCompressedArrays[i] << endl;
		for (int cc = 0; cc< lengthOfCompressedArrays[i]; cc++) {
			printf("%d", *(byteArrays[i] + cc) & 0xff);
		}
		cout << endl;
		*/

	}
	
	out.close();
	return true;
	
}


bool fileOfNumOrString2CompressedDataFile(string sourceFile, string dp, string destFile) {
	vector<Byte *> byteArrays;
	vector<int> lengthOfCompressedArrays;
	bool success1 = fileOfNumOrString2CompressedByteArrays(sourceFile, dp, byteArrays,lengthOfCompressedArrays);	
	if (success1) {
		bool success = compressedDataToFile(byteArrays, lengthOfCompressedArrays, dp, destFile);
		return success;
	} else {
		return false;
	}
	

	for (int i=0;i<byteArrays.size();i++) {
		delete [] byteArrays[i];
	}
	
}




