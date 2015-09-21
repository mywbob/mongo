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


using namespace std;

void compressData(Byte* input, int inputlen, Byte* output, int &outputlen);
void decompressData(Byte* input, int inputlen, Byte* output, int &outputlen);

