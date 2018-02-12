#pragma once
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

class threadpool {
	std::vector<std::thread> pool_;
	std::queue<std::function<void()>> tasks_queue_;
	std::mutex m_task;
	std::condition_variable notEmpty_;
	std::condition_variable notFull_;
	bool stop_;

public:
	threadpool(size_t num_threads);
	~threadpool();
	std::function<void()> get_task();
	void do_task();
	void add_task(std::function<void()> task);
	void terminal();
};

