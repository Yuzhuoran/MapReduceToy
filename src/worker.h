#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <mr_task_factory.h>
#include "mr_tasks.h"
//#include "threadpool.h"

#include <grpc++/grpc++.h>
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc/support/log.h>

#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;

using masterworker::Comm;
using masterworker::controlMessage;
using masterworker::replyMessage;
using masterworker::fileInfo;

#include "masterworker.grpc.pb.h"

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:

		std::string ip_addr_port;

		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		~Worker() {
			server_->Shutdown();
			cq_->Shutdown();
		}

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		Comm::AsyncService service_;
		std::unique_ptr<ServerCompletionQueue> cq_;
		std::unique_ptr<Server> server_;

		class CallData {
			private:
				Comm::AsyncService* service_;
				ServerCompletionQueue* cq_;
				ServerContext ctx_;
				controlMessage request_;
				replyMessage reply_;
				ServerAsyncResponseWriter<replyMessage> responder_;
				enum CallStatus
				{
					CREATE, PROCESS, FINISH
				};
				CallStatus status_;
			public:
				CallData(Comm::AsyncService* service, ServerCompletionQueue* cq)
				: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE)
				{
					Proceed();
				}

				void Proceed(){
					if(status_ == CREATE){
						status_ = PROCESS;
						service_->RequestAssignTask(&ctx_, &request_, &responder_, cq_, cq_, this);
					}else if (status_ == PROCESS){
						new CallData(service_, cq_);

						//get the parameter from request
						uint32_t num_output = request_.numofoutput();
						uint32_t type = request_.type();
						uint32_t worker_id = request_.workid();

						std::string out_dir= request_.outputdir();
						std::string user_id = request_.userid();

						/*
						std::cout << "userid: " <<user_id << " ";
						std::cout << "output: " << num_output << " ";
						std::cout << "type: " << type << " ";
						//std::cout << "out_dir:" << out_dir << std::endl;
						*/
						std::shared_ptr<BaseMapper> mapper = get_mapper_from_task_factory(user_id);
    					std::shared_ptr<BaseReducer> reducer = get_reducer_from_task_factory(user_id);

						//type ==1 map 
						if (type == 1){
							std::string path = out_dir + "intermediate/" + "intermediate_" + std::to_string(worker_id);
							std::cout << "intermediate path: "<< path << std::endl;
							for(auto &f: request_.mapfiles()){
								ifstream file(f.filename());
								if(file.is_open()){
									file.seekg(f.fileoffset(), ios::beg);
									std::string l;
									std::getline(file, l);

									// continue read the lines
									while(file.tellg() <= f.fileoffset() + f.filesize() && file.tellg() != -1){
										
										//std::cout << "line:" << l << std::endl
										mapper->impl_->initialize(user_id, path, num_output);
										mapper->map(l);
										std::getline(file, l);
									}
									file.close();
								}else{	
									std::cout<<"cant open the file" << std::endl;
									return;
								}
							}
							// get the intermediate file name from the mapper
							for(auto &it : mapper->impl_->get_name_list()){
								reply_.add_outputfiles(it);
							}

						}else if(type == 2){
							std::string path = "./" + out_dir  + "output" + '/' + "out_" + std::to_string(worker_id);
							std::cout << "reduce path: " << path << std::endl;
							std::map<std::string, std::vector<std::string> > key_val_count;
							for(auto &f: request_.reducefiles()){
								ifstream file(f.filename());
								int count = 0;
								if(file.is_open()){
									std::string l;
									while(std::getline(file, l)){
										reducer->impl_->initialize(user_id, path, num_output);
										int i = 0;
										for(i = 0; i < l.size(); i++){
											if(l[i] == ' ')
												break;
										}
										key_val_count[l.substr(0, i)].push_back(l.substr(i+1, l.size()-i-1));
									}
									file.close();
								}else{
									std::cout<<"cant open the file" << std::endl;
									return;
								}
							}
							// take the reduce work
							for(auto &it : key_val_count){
								reducer->reduce(it.first, it.second);
							}
							// add to the output file name, but it is not used in the project
							for(auto &it : reducer->impl_->get_name_list()){
								reply_.add_outputfiles(it);
							}

						}
						status_ = FINISH;
						responder_.Finish(reply_, Status::OK, this);
					}else{
						GPR_ASSERT(status_ == FINISH);
						delete this;
					}
				}
		};

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	ServerBuilder builder;
	builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
	builder.RegisterService(&service_);
  	cq_ = builder.AddCompletionQueue();
  	server_ = builder.BuildAndStart();
	std::cout << "Server listening on " << ip_addr_port << std::endl;
}



/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	new CallData(&service_, cq_.get());
	void* tag;
	bool ok;
	while(true){
		GPR_ASSERT(cq_->Next(&tag, &ok));
		GPR_ASSERT(ok);
		static_cast<CallData*>(tag)->Proceed();
	}
	
	return true;
}
