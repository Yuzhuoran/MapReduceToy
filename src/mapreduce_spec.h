#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <sstream>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	uint32_t n_workers;
	vector<string> worker_ipaddr_ports;
	vector<string> input_files;
	string output_dir;
	uint32_t n_output_files;
	uint32_t map_kilobytes;
	string user_id;
	uint32_t n_mappers;
};

inline uint32_t getNumMappers(vector<string> input_files, uint32_t map_kilobytes) {
	int total = 0;
	for(string filename : input_files) {
		struct stat st;
		if(stat(filename.c_str(), &st) == 0) {
			total += st.st_size;
		}
  	}
	return (uint32_t)(total / map_kilobytes + 1);
	
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	ifstream configFileStream;
	string line;
	string key;
	string value;
	mr_spec.n_workers = 0;
	mr_spec.n_output_files = 0;
	mr_spec.map_kilobytes = 0;
	mr_spec.n_mappers = 0;
	configFileStream.open(config_filename);
	while(getline(configFileStream, line)) {
		istringstream lineStream(line);
		if(getline(lineStream, key, '=') && getline(lineStream, value)) {
			if(key.compare("n_workers") == 0) {
				mr_spec.n_workers = stoi(value);
			} else if (key.compare("worker_ipaddr_ports") == 0) {
				string addr;
				istringstream stream(value);
				while(getline(stream, addr, ',')){
					mr_spec.worker_ipaddr_ports.push_back(addr);
	   			}
			} else if (key.compare("input_files") == 0) {
				string filename;
				istringstream stream(value);
				while(getline(stream, filename, ',')){
					mr_spec.input_files.push_back(filename);
	   			}
			} else if (key.compare("output_dir") == 0) {
				mr_spec.output_dir = value;
			} else if (key.compare("n_output_files") == 0) {
				mr_spec.n_output_files = stoi(value);
			} else if (key.compare("map_kilobytes") == 0) {
				mr_spec.map_kilobytes = stoi(value) * 1024;
			} else {
				;
			}
		}
	}
	mr_spec.n_mappers = getNumMappers(mr_spec.input_files, mr_spec.map_kilobytes);
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if(mr_spec.n_workers <= 0) {
		cout << "Workers number error!" << endl;
		return false;
	}
	if((mr_spec.n_output_files <= 0)) {
		cout << "Output files number error!" << endl;
		return false;
	}
	if((mr_spec.map_kilobytes <= 0)) {
		cout << "Output files number error!" << endl;
		return false;
	}
	for(string filename : mr_spec.input_files) {
		struct stat st;
		if(stat(filename.c_str(), &st) != 0) {
			cout<<"Input file error: cannot access "<< filename << endl;
			return false;
		}
  	}
	struct stat st;
	if (stat(mr_spec.output_dir.c_str(), &st) || S_ISDIR(st.st_mode) == 0) {
		cout << "Output directory error" << endl;
		return false;
	}
	return true;
}
