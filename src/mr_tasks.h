#pragma once

#include <string>
#include <iostream>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::vector<std::string> fileinfos;
		std::string path;
		unsigned hash_key(std::string &s);
		unsigned num_emit;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::file_name = path + std::to_string(hash_key(key) % num_emit);
	std::ofstream fs;
	fs.open(file_name, std::ofstream::app);
	if(fs.is_open()){
		std::string pair = key + " " + val +"\n";
		fs.write(pair.c_str(), pair.size());
		fs.close();
		return;
	}else{
		std::cout<< "file can't open" << std::endl;
	}
	std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
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
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
