#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <vector>
#include <set>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void initialize(std::string id, std::string path, int num);
		std::vector<std::string> get_name_list();
		void test(){
			std::cout <<"this function runs!" << std::endl;
		}
		// user_id and out_dir, prefix, id
		std::string user_id;
		std::string prefix;
		std::string out_dir;

		// check if the file we have already create
		std::vector<std::string> file_names;
		std::set<std::string> file_exists;

		uint32_t num_out;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	std::string file_name = out_dir + std::to_string((int) key[0] % num_out);
	std::ofstream fs;
	fs.open(file_name, std::ofstream::app);
	if(fs.is_open()){
		std::string pair = key + " " + val + "\n";
		fs.write(pair.c_str(), pair.size());
		// if the file first appear
		if(file_exists.find(file_name) == file_exists.end()){
			std::cout << "map filename: " << file_name << std::endl;
			file_exists.insert(file_name);
		}
		fs.close();
		return;
	}else{
		std::cout<< "file can't open" << std::endl;
	}
	
	return;
}

inline void BaseMapperInternal::initialize(std::string id, std::string path, int num){
	
	user_id = id;
	out_dir = path;
	num_out = num;
	
}

inline std::vector<std::string> BaseMapperInternal::get_name_list(){
	std::vector<std::string> res;
	for(auto &name : file_exists){
		res.push_back(name);
	}
	return res;
}
/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void initialize(std::string id, std::string path, int num);
		std::vector<std::string> get_name_list();

		std::string user_id;
		std::string out_dir;

		std::vector<std::string> file_names;
		// check if the file has proceeded;

		std::set<std::string> file_exists;
		int num_out;

};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::string file_name = out_dir;
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	std::ofstream fs;
	fs.open(file_name, std::ofstream::app);
	if(fs.is_open()){
		std::string pair = key + " " + val + "\n";
		fs.write(pair.c_str(), pair.size());
		// if the file first appear, store it and output later
		if(file_exists.find(file_name) == file_exists.end()){
			std::cout<<"reduce emit filename" << file_name << std::endl;
			file_exists.insert(file_name);
		}
		fs.close();
		return;
	}else{
		std::cout<< "file can't open" << std::endl;
	}
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	return;
}

inline void BaseReducerInternal::initialize(std::string id, std::string path, int num){
	
	user_id = id;
	out_dir = path;
	num_out = num;
}

inline std::vector<std::string> BaseReducerInternal::get_name_list(){
	std::vector<std::string> res;
	for(auto &name : file_exists){
		res.push_back(name);
	}
	return res;
}