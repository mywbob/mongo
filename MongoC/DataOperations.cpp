#include "DataOperations.h"

void compressData(Byte* input, int inputlen, Byte* output, int &outputlen) {
	//debug
	//printf("in compressData, before compress, the output addr is %d\n", output);
	
	z_stream defstream;
	defstream.zalloc = Z_NULL;
	defstream.zfree = Z_NULL;
	defstream.opaque = Z_NULL;
	defstream.avail_in = (uInt)inputlen; // size of input
	defstream.next_in = (Bytef *)input; // input byte array
	defstream.avail_out = (uInt)(inputlen + 20); // size of output, larger for small input
	defstream.next_out = (Bytef *)output; // output byte array
	deflateInit2(&defstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 16+MAX_WBITS, 8, Z_DEFAULT_STRATEGY);//try to change the level later
	deflate(&defstream, Z_FINISH);
	deflateEnd(&defstream);
	
	//debug: size of the output
	/*
	printf("next out %lu\n", (Byte*)defstream.next_out);// the last addr of compressed data
	printf("start addr %lu\n", output ); //the starting addr of compressed data
	printf("Deflated size is: %lu\n", ((Byte*)defstream.next_out - output));
	*/
	
	//compressed data len
	outputlen = (Byte*)defstream.next_out - output;
	
	
	//debug
	//printf("in compressData, after compress, the output addr is %d\n", output);
}

void decompressData(Byte* input, int inputlen, Byte* output, int &outputlen) {
	z_stream infstream;
	infstream.zalloc = Z_NULL;
	infstream.zfree = Z_NULL;
	infstream.opaque = Z_NULL;
	infstream.avail_in = (uInt)inputlen; // size of input
	infstream.next_in = (Bytef *)input; // input byte array
	infstream.avail_out = (uInt)1000; // size of output-------------change this, we do not know the size after decompress, maybe make this big enough 
	infstream.next_out = (Bytef *)output; // output byte array
	inflateInit2(&infstream, 16+MAX_WBITS);
	inflate(&infstream, Z_NO_FLUSH);
	inflateEnd(&infstream);
	printf("inflate finished\n");
}