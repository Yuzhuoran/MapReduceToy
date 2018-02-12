#pragma once

#include <vector>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
// just store the offset of the file, no new file created
// the shard structure should be a vector of file: (filename, file offset)
// there are M shards, use a vector to store

struct FileInfo {
	std::string filename;
	uint32_t offset;
	uint32_t size;
};

struct FileShard {
	std::vector<FileInfo> fileInfos;

};

inline bool get_fileinfos(){

	
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	uint32_t shard_size = mr_spec.map_kilobytes;
	uint32_t num_shard = mr_spec.n_mappers;
	uint32_t num_cur = 0;
	while(cur < num_shard){
		FileShard fileshard;
		uint32_t size_cur = 0;
		while(size_cur < shard_size){
			// file info to addin to the shards
			FileInfo fileinfo;
			uint32_t size_remain = shard_size - size_cur;






		}



	}



	return true;
}
