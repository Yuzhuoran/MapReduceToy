#include "threadpool.h"
#include <iostream>

threadpool::threadpool(size_t num_threads)
	: stop_(false){
	for(size_t i = 0; i < num_threads; i++){
		pool_.push_back(std::thread(&threadpool::do_task, this));
	}
}

threadpool::~threadpool(){
	if(!stop_){
		terminal();
	}
}

// get a task from the task queue
std::function<void()> threadpool::get_task(){
	// critival section
	std::unique_lock<std::mutex> lock(m_task);
	while(tasks_queue_.empty() && !stop_){
		notEmpty_.wait(lock);
	}
	std::function<void()> task;
	if(!tasks_queue_.empty()){
		task = tasks_queue_.front();
		tasks_queue_.pop();
		notFull_.notify_one();
	}
	return task;
}

// do work for a thread
void threadpool::do_task(){
	//enter critical section
	while(!stop_){
		std::function<void()> task = get_task();
		if(task){
			task();
			//std::thread::id this_id = std::this_thread::get_id();
			//std::cout<<"thread id"<<this_id<<std::endl;
		}
	}
}

void threadpool::add_task(std::function<void()> task){
	std::unique_lock<std::mutex> lock(m_task);
	while(tasks_queue_.size() >= pool_.size() && !stop_){
		notFull_.wait(lock);
	}
	if(tasks_queue_.size() < pool_.size()){
		tasks_queue_.push(task);
		notEmpty_.notify_one();
	}
	return;
}

void threadpool::terminal(){
	std::unique_lock<std::mutex> lock(m_task);
	stop_ = true;
	notEmpty_.notify_all();
	for(std::thread &t : pool_){
		t.join();
	}
}