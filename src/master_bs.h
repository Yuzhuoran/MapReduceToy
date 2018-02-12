#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <string>
#include <fstream>
#include <ssteam>
#include <vector>
#include <queue>
#include <thread>

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
	timeval StartTime;
};

struct worker
{
	int id;
	int status;
	std::string worker_ipaddr_port;
	
};

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
		// reference client.cc
		uint32_t num_mapper, num_reducer;
		uint32_t num_worker;

		MapReduceSpec mr_spec;
		std::vector<map_task> map_task_arr;
		std::vector<reduce_task> reduce_task_arr;
		std::vector<MasterClient*> clientInterface;
		std::vector<FileShard> file_shards;

		std::map<uint32_t,task> trackTask;
		std::deque<Task> taskToDo;
		std::deque<uint32_t> worker_ready;

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

			void Assign_mapper(uint32_t task_type, FileShard file_shard){
				// assign a map task for a worker
				// if task == 1 map  == 0 reduce
				caller_ = new AsyncClientCaller;
				controlMessage request;
				// set request information

				// rpc calls 
				reader_ = stub_->PrepareAsynAssign_task(&(caller->context), request, &(caller->cq));
				reader_->StartCall();
				rpc->Finish(&reply_, &status_, (void*)1);
				busy = true;
				
			}

			void Assign_reducer(){


			}

			bool Check_status(std::vector<std::string>& mapOutputFiles){
				// check if the rpc calls finish
				// if finish, then write the information to the output file and return true;
				// else return false;
				void* got_tag;
				bool ok = false;
				std::chrono::system_clock::time_point delay = std::chrono::system_clock::now() + 
				GPR_ASSERT(cq_.AsynNext<std::chrono::system_clock::time_point> (&got_tag, &ok, delay));
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
					mapOutputFiles.push_back(caller_->reply.mapOutputFiles(i));
				}
				
				busy_ = false;
				return true
			}

		
		private:			
			std::shared_ptr<Comm::Stub> stub_;
			AsyncClientCaller* caller_;
			bool busy_;
		}


		
};



/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& spec, const std::vector<FileShard>& file_shards) {
	num_worker = spec.n_workers;
	num_reducer = spec.n_output_files;
	num_mapper = file_shards.size();


}


//assign the map to worker
Master::assign_map(uint32_t num_out, FileShard fileshard){
	// assign the map task to the worker, 
	if(worker_ready.size() > 0){
		uint32_t worker_inx = worker.pop_front();
		// keep track of each task
		trackTask[worker_inx] = work;
		clientInterface[worker_inx]->Assign_mapper(task);

	}else{
		taskToDo.push_front(task);
	}
}

Master::assign_reduce(){



}

Master::task_end(std::vector<std::string>& outfile){
	// check is the map or reduce tasks are finished by workers
	for(uint32_t i = 0; i < num_worker; i++){

		if(){

		}
		// if the worker finish the task, put another task into it 
		if(clientInterface[i]->Check_status(outfile)){
			if(taskToDo.size() > 0){
				Task task = taskToDo.pop_front();
				trackTask[i] = task;
				clientInterface[i]->
			}

		}
	}

}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	//Map tasks
	
	//assign the information for reduce array

	
}