#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <queue>
#include <thread>
#include <set>
#include <chrono>
#include <sys/time.h>

#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include <grpc/support/log.h>

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
using masterworker::fileInfo;


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */

// define a task 
struct Task{
	// type =1 is map type =2 is reduce
	uint32_t type;
	uint32_t num_out;
	FileShard fileshard;
	std::string out_dir;
	std::string id;
};

// busy not use?
class Master {
	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();
		void assign_map(uint32_t num_inter, FileShard& fileshard, uint32_t& work_id);
		void assign_reduce(uint32_t num_out, FileShard& inter_files, uint32_t& work_id);

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		uint32_t num_mapper, num_reducer;
		uint32_t num_worker;

		
		//fileshards			bool busy_;
		std::vector<FileShard> file_shards;

		//task to assign by the master
		std::deque<Task> taskToDo;

		//available worker
		std::deque<uint32_t> worker_ready;

		//busy worker store in a set
		std::set<uint32_t> worker_working;

		//a caller structure
		struct AsyncClientCaller{
			replyMessage reply;
			ClientContext context;
			Status status;
			std::unique_ptr<ClientAsyncResponseReader<replyMessage> > reader;
		};

		class MasterClient{
		public:
			explicit MasterClient(std::shared_ptr<Channel> channel)
				: stub_(Comm::NewStub(channel)) {
				}

			// make rpc calls to let worker do the map
			void rpc_mapper(Task task, uint32_t work_id){
				//assign the map task to worker
				std::cout << work_id << " assign map starts" << std::endl;
				caller = new AsyncClientCaller;
				controlMessage request;
				//type == 1 map 
				request.set_type(1);
				// output direction
				request.set_outputdir(task.out_dir);
				// worker id for file
				request.set_workid(work_id);
				request.set_numofoutput(task.num_out);
				request.set_userid(task.id);

				//set fileinfo to rpc calls
			  	for(auto &f: task.fileshard.files){
			  		fileInfo* info = request.add_mapfiles();
			  		info->set_filename(f.filename);
			  		info->set_filesize(f.size);
			  		info->set_fileoffset(f.start);
			  	}

			  	//prepare and start call
				caller->reader = stub_->PrepareAsyncAssignTask(&(caller->context), request, &cq);
				caller->reader->StartCall();
				caller->reader->Finish(&(caller->reply), &(caller->status), (void*)1);
				//std::cout << "call finish " << std::endl;
				std::cout << work_id << " assign map finish" << std::endl;
				
				
			}

			// make rpc calls to let worker to do the reduce
			void rpc_reducer(Task task, uint32_t work_id){
				caller  = new AsyncClientCaller;
				controlMessage request;
				std::cout << work_id << " assign reduce starts" << std::endl;
				request.set_type(2);
				request.set_outputdir(task.out_dir);
				request.set_workid(work_id);
				request.set_numofoutput(task.num_out);
				request.set_userid(task.id);
				

			  	for(auto &f: task.fileshard.files){
			  		fileInfo* info = request.add_reducefiles();
			  		info->set_filename(f.filename);
			  		info->set_filesize(f.size);
			  		info->set_fileoffset(f.start);
			  	}
			  
				//request.set_
				caller->reader = stub_->PrepareAsyncAssignTask(&(caller->context), request, &cq);
				caller->reader->StartCall();
				caller->reader->Finish(&(caller->reply), &(caller->status), (void*)1);
				//busy = true;
				//std::cout << "call finish " << std::endl;
				std::cout << work_id << " assign reduce finish" << std::endl;
				
			}

			// check the worker status and store the reply files in the outputfiles, the output file is the name of intermediate files or final output file
			bool check_status(std::vector<std::string>& Output){
				
				void* got_tag;
				bool ok = false;
   				std::chrono::system_clock::time_point delay = std::chrono::system_clock::now() + std::chrono::duration<int,std::ratio<1> > (1);
				GPR_ASSERT(cq.AsyncNext<std::chrono::system_clock::time_point> (&got_tag, &ok, delay));
				// check if the event finish
				if (got_tag == NULL)
					return false;
				GPR_ASSERT(ok);
				if(!caller->status.ok()){
					std::cout << caller->status.error_code() << ": " << caller->status.error_message() << std::endl;
					return false;
				}
				// if the maper task finish, store in the mapOutputFiles and return true;
				for(int i = 0; i < caller->reply.outputfiles_size(); i++){
					Output.push_back(caller->reply.outputfiles(i));
				}
				return true;
			}
		private:			
			std::shared_ptr<Comm::Stub> stub_;
			AsyncClientCaller* caller;
			CompletionQueue cq;
		};	
	private:
		//interface to make rpc calls
		std::vector<MasterClient*> clientInterface;

};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& spec, const std::vector<FileShard>& file_shards)
:file_shards(file_shards){
	//initialize the master 
	num_worker = spec.n_workers;
	num_reducer = spec.n_output_files;
	num_mapper = spec.n_mappers;
	for(uint32_t i = 0; i < num_worker; i++){
		std::shared_ptr<Channel> channel = grpc::CreateChannel(spec.worker_ipaddr_ports[i], grpc::InsecureChannelCredentials());
		clientInterface.push_back(new MasterClient(channel));
		worker_ready.push_back(i);
	}
	std::cout << "num_worker: " << num_worker << std::endl; 
	std::cout << "num_mapper: " << num_mapper << std::endl; 
	std::cout << "num_reducer: " << num_reducer << std::endl;
	std::cout << "shard_size: " << file_shards.size() << std::endl;
	std::cout << std::endl;

}

//assign the map to worker
void Master::assign_map(uint32_t num_inter, FileShard& fileshard, uint32_t& work_id){
	//  if there is available worker, assgin the map task
	Task task = {1, num_inter, fileshard, "", "cs6210"};
	if(worker_ready.size() > 0){
		uint32_t i = worker_ready.front();
		worker_ready.pop_front();
		clientInterface[i]->rpc_mapper(task, work_id);
		work_id++;
		worker_working.insert(i);
	}else{
		taskToDo.push_back(task);
	}
}

//assign the reduce to worker
void Master::assign_reduce(uint32_t num_out, FileShard& inter_files, uint32_t& work_id){
	// if there is available worker, assign the reduce task
	Task task = {2, num_out, inter_files, "", "cs6210"};
	if(worker_ready.size() > 0){
		uint32_t i = worker_ready.front();
		worker_ready.pop_front();
		clientInterface[i]->rpc_reducer(task, work_id);
		work_id++;
		worker_working.insert(i);
	}else{
		taskToDo.push_back(task);
	}
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	//Map tasks
	std::vector<std::string> intermediate_files;
	
	
	uint32_t work_id = 0;
	for(int i = 0; i < file_shards.size(); i++){
		//std::cout<< "ready worker: " << worker_ready.size() << std::endl;
		assign_map(num_mapper, file_shards[i], work_id);
		std::cout << "worker id:" << work_id << std::endl;
	}
	while(taskToDo.size() > 0){
		std::cout << "map task to do " << taskToDo.size() << std::endl;
		for(int i = 0; i < num_worker; i++){
			//if the worker not in the working set, continue
			if(worker_working.find(i) == worker_working.end())
				continue;
			bool done = clientInterface[i]->check_status(intermediate_files);
		    if (done){
		    	worker_working.erase(i);
			    if (taskToDo.size() > 0){
			    	//std::cout << "worker id after done: "<< work_id << std::endl; 
			        Task task = taskToDo.front();
			        taskToDo.pop_front();
			        worker_working.insert(i);
			        clientInterface[i]->rpc_mapper(task, work_id);
			        std::cout << "worker id:" << work_id << std::endl;
			        work_id ++;
			      }else{
			        worker_ready.push_back(i);
			      }
		 	}
		}
	}
	

	//wait for the map phase to finish all because the result may not return yet
	while(true){
		bool stop = true;
		for(int i = 0; i < num_worker; i++){
			if(worker_working.find(i) == worker_working.end())
				continue;
			if (!clientInterface[i]->check_status(intermediate_files))
				stop = false;
		}
		if(stop)
			break;
	}
	std::cout << "map phase finish " << std::endl;
	std::vector<std::string> output_files;
	std::vector<FileShard> reduce_tasks;
	reduce_tasks.resize(num_reducer);
	
	// reduce phase, for simplify, we create the number of reducer for the number of shards, and resize the intermediate files into these shards
	// that a shard contains several intermediate files
	for (int i = 0; i < intermediate_files.size(); i++)
	{
	    File segment;
	    segment.filename = intermediate_files[i];
	    uint32_t index = i % num_reducer;
	    reduce_tasks[index].files.push_back(segment);
	}
	
	// initialize 
	cout << "inter file size: " << intermediate_files.size()<<std::endl;
	std::cout << "change1" << std::endl;
	taskToDo.clear();
	worker_working.clear();
	worker_ready.clear();

	//similar process for reduce
	work_id = 0;
	for(int i = 0; i < num_worker; i++)
		worker_ready.push_back(i);

	// assign reduce tasks, similar to the previous one
	for(int i = 0; i < reduce_tasks.size(); i++){
		assign_reduce(num_reducer, reduce_tasks[i], work_id);
	}

	while(taskToDo.size() > 0){
		std::cout << "reduce task to do" << taskToDo.size() << std::endl;
		for(uint32_t i = 0; i < num_worker; i++){
			//if the current worker not returing rpc reply, skip
			if(worker_working.find(i) == worker_working.end())
				continue;
			bool done = clientInterface[i]->check_status(output_files);
		    if (done){
		    	worker_working.erase(i);
			    if (taskToDo.size() > 0){
			        Task task = taskToDo.front();
			        taskToDo.pop_front();
			        worker_working.insert(i);
			        clientInterface[i]->rpc_reducer(task, work_id);
			        work_id ++;
			      }else{
			        worker_ready.push_back(i);
			      }
		 	}
		}
	}
	while(true){
		bool stop = true;
		for(int i = 0; i < num_worker; i++){
			if(worker_working.find(i) == worker_working.end())
				continue;
			if (!clientInterface[i]->check_status(output_files))
				stop = false;
		}
		if(stop)
			break;
	}
	
	return true;
}