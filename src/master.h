#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <string>
#include <fstream>
#include <ssteam>
#include <vector>
#include <queue>
#include <thread>
#include <set>
#include <chrono>
#include <sys/time.h>

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pn.h"

using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Channel;
using grpc::Status;
using grpc::ServerContext;
using grpc::ClientContext;

using masterworker::Comm;
using masterworker::controlMessage;
using masterworker::replyMessage;
using masterworker::fileInfo


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

struct Task{
	uint32_t type;
	uint32_t num_out;
	FileShard file;
};

// busy not use?
class Master {
	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();
		void assign_map();
		void assign_reduce();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		uint32_t num_mapper, num_reducer;
		uint32_t num_worker;

		MapReduceSpec mr_spec;;
		//interface to make rpc calls
		std::vector<MasterClient*> clientInterface;

		//fileshards			bool busy_;
		std::vector<FileShard> file_shards;

		//task to assign by the master
		std::deque<Task> taskToDo;

		//available worker
		std::deque<uint32_t> worker_ready;

		//busy worker
		std::set<uint32_t> worker_working;

		//a caller structure
		struct AsyncClientCaller{
			replyMessage reply;
			ClientContext context;
			Status status;
			std::unique_ptr<ClientAsyncResponseReader<replyMessage> > reader;
		}

		class MasterClient{
		public:
			explicit MasterClient(std::shared_ptr<Channel> channel)
				: stub_(Comm::NewStub(channel)) {
				}

			// make rpc calls to let worker do the map
			void rpc_mapper(Task task){
				//assign the map task to worker
				caller_ = new AsyncClientCaller;
				controlMessage request;
				// set request information
				//request.set_
				// rpc calls 
				reader_ = stub_->PrepareAsynrpc_mapper(&(caller->context), request, &(caller->cq));
				reader_->StartCall();
				rpc->Finish(&(caller->reply), &(caller->status), (void*)1);
				//busy = true;
				
			}

			// make rpc calls to let worker to do the reduce
			void rpc_reducer(Task task){
				caller_ = new AsyncClientCaller;
				controlMessage request;
				
				//request.set_

				reader_ = stub_->PrepareAsynrpc_reducer(&(caller->context), request, &(caller->cq));
				reader_->StartCall();
				rpc->Finish(&(caller->reply), &(caller->status), (void*)1);
				//busy = true;

			}

			// check the worker status and store the reply files in the outputfiles
			bool Check_status(std::vector<std::string>& OutputFiles){
				void* got_tag;
				bool ok = false;

				std::chrono::duration<int,std::ratio<1> > one_second (1);
   				std::chrono::system_clock::time_point timeTo = std::chrono::system_clock::now() + one_second;
				GPR_ASSERT(cq.AsyncNext<std::chrono::system_clock::time_point> (&got_tag, &ok, delay));
				// check if the event finish
				if (got_tag == NULL)
					return false;
				GPR_ASSERT(ok);
				if(!caller->status.ok()){
					std::cout << caller_->status.error_code() << ": " << caller_->status.error_message() << std::endl;
					return false;
				}
				// if the maper task finish, store in the mapOutputFiles and return true;
				int size = caller_->reply.mapOutputFiles_size();
				for(int i = 0; i < size; i++){
					OutputFiles.push_back(caller_->reply.mapOutputFiles(i));
				}
				
				//busy_ = false;
				return true
			}
		private:			
			std::shared_ptr<Comm::Stub> stub_;
			AsyncClientCaller* caller;
			CompletionQueue cq;
			//bool busy_;
		}
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& spec, const std::vector<FileShard>& file_shards)
:file_shards(file_shards), mr_spec(spec) {
	//initialize the master 
	num_worker = spec.n_workers;
	num_reducer = spec.n_output_files;
	num_mapper = spec.n_mappers;
	for(uint32_t i = 0; i < num_worker; i++){
		clientInterface.push_back(new MasterClient(worker_ipaddr_ports[i]));
		worker_ready.push_back(i);
	}
}

//assign the map to worker
Master::assign_map(uint32_t num_inter, FileShard& fileshard){
	//  if there is available worker, assgin the map task
	if(worker_ready.size() > 0){
		Task task = {1, num_inter, fileshard};
		uint32_t i = worker_ready.front();
		worker_ready.pop_front();
		clientInterface[i]->rpc_mapper(task);
		worker_working.insert(i);
	}else{
		taskToDo.push_back(task);
	}
}

//assign the reduce to worker
Master::assign_reduce(uint32_t num_out, FileShard& inter_files){
	// if there is available worker, assign the reduce task
	if(worker_ready.size() > 0){
		Task task = {2, num_out, inter_files};
		uint32_t i = worker_ready.front();
		worker_ready.pop_front();
		clientInterface[i]->rpc_reducer(task);
		worker_working.insert(i);
	}else{
		taskToDo.push_back(task);
	}
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	//Map tasks
	std::vector<std::string> intermediate_files;
	std::vector<std::string> output_files;
	std::vector<FileShard> reduce_tasks;
	reduce_tasks.resize(num_reducer);

	for(int i = 0; i < file_shards.size(); i++){
		assign_map(num_mapper, file_shards[i]);
	}
	while(taskToDo.size() > 0){
		for(int i = 0; i < num_worker; i++){
			//if the worker not in the working set, continue
			if(worker_working.find(i) == worker_working.end())
				continue;
			bool done = clientInterface[i]->check_status(intermediate_files);
			// if finish, assign a new one
		    if (done){
		    	worker_working.erase(i);

		    	// if there are tasks to do

			    if (taskToDo.size() > 0){
			        Task task = taskToDo.pop_front();
			        worker_working.insert(i);
			        clientInterface[i]->rpc_mapper(task);
			      }else{
			        worker_ready.push_back(i);
			      }
		 	}
		}
	}
	/*
	// reduce phase, first create the task with file_shards, one shard is a file for simplify
	for (uint32_t i = 0; i < intermediate_files.size(); i++)
	{
	    PerFileInfo tempFileInfo;
	    tempFileInfo.fileName = intermediate_files[i];
	    uint32_t index = i % num_reducer;
	    reduce_tasks[index].fileInfo.push_back(tempFileInfo);
	}
	*/
	// initialize 
	taskToDo.clear();
	worker_working.clear();
	worker_ready.clear();
	for(int i = 0; i < num_worker; i++)
		worker_ready.push_back(i);

	// assign reduce tasks, similar to the previous one
	for(int i = 0; i < reduce_tasks.size(); i++){
		assign_reduce(num_reducer, reduce_tasks[i]);
	}
	while(taskToDo.size() > 0){
		for(uint32_t i = 0; i < num_worker; i++){
			//if the current worker not returing rpc reply, skip
			if(worker_working.find(i) == worker_working.end())
				continue;
			bool done = clientInterface[i]->check_status(intermediate_files);
		    if (done){
		    	worker_working.erase(i);
			    if (taskToDo.size() > 0){
			        Task task = taskToDo.pop_front();
			        worker_working.insert(i);
			        clientInterface[i]->rpc_reducer();
			      }else{
			        worker_ready.push_back(i);
			      }
		 	}
		}
	}

	return true;
}