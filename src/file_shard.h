#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"

#define KB 1024

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
typedef struct FileInfo {
	uint32_t start;
	uint32_t size;
	string filename;
} File;

typedef struct FileShard {
	uint32_t count;
	vector<File> files;
} FileShard;


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int i = 0;
	vector<fstream*> fileStreams;
	vector<uint32_t> fileSizes;
	//uint64_t total = 0;
	double remain = 0.0;
	uint32_t shardCount = 0;
  	uint32_t fileNum = 0;
	//uint32_t fileId = 0;
	//uint64_t offset = 0;
	FileShard shard;
	shard.count = 0;
	int temp = 0;

	while (i < mr_spec.input_files.size()) {
		uint32_t localSize = 0;
		fstream* f = new fstream (mr_spec.input_files[i].c_str(),fstream::in);
		if (f == NULL) {
			cout << "Cannot open file: " << mr_spec.input_files[i] << endl;
			exit(1);
		} else {
			f->seekg(0, f->end);
    			localSize = f->tellg();
    			//total += localSize;
			remain += localSize;
    			f->seekg(0,f->beg);
			fileStreams.push_back(f);
			fileSizes.push_back(localSize);
		}
		i += 1;
	}

	while(remain > 0)
	{
		int localSize = 0;
		if (remain <= mr_spec.map_kilobytes) {
			localSize = (int)remain;
		} else {
			localSize = mr_spec.map_kilobytes;
		}
    		shard.count = shardCount;
		shardCount += 1;
    		shard.files.clear();
    		while(localSize > 0)
    		{
			string line;
			File tempFile;
      			if ((temp = (int) fileSizes[fileNum] - fileStreams[fileNum]->tellg()) == 0)
      			{
        			fileNum += 1;
        			continue;
      			}

			tempFile.filename = mr_spec.input_files[fileNum];
        		tempFile.start = fileStreams[fileNum]->tellg();
     			if (temp <= localSize) //Case where the file is enough
      			{
				tempFile.size = (uint32_t)(fileSizes[fileNum] - tempFile.start);
        			fileStreams[fileNum]->seekg(0, fileStreams[fileNum]->end);
        			localSize -= tempFile.size;
      			}
      			else
      			{
        			fileStreams[fileNum]->seekg(tempFile.start + localSize);
        			getline(*(fileStreams[fileNum]), line);
        			tempFile.size = (uint32_t)((int)fileStreams[fileNum]->tellg() - (int)tempFile.start);
        			localSize = 0;
      			}
			remain -= tempFile.size;
        		shard.files.push_back(tempFile);
    		}
    		fileShards.push_back(shard);
  	}


	return true;
}
