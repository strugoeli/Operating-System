

/*
 * Singleton class of ThreadManager, manage all the operation between threads in the static uthreads library
 */

#ifndef OS_EX2_THREADMANAGER_H
#define OS_EX2_THREADMANAGER_H


//---------------Include----------------------//
#include <signal.h>
#include <algorithm>
#include <list>
#include <iostream>
#include <memory>
#include <array>
#include <queue>
#include "Thread.h"
//#include "uthreads.h"


//---------------MACRO-----------------------//

#define JB_SP 6
#define JB_PC 7
#define MAX_THREAD_NUM 100
#define SUCCESS 0
#define FAILURE (-1)
#define READY 0
#define RUNNING 1
#define BLOCKED 2

/*
 * ThreadManager class - singleton
 */
class ThreadManager
{
    using priority_queue = std::priority_queue<int, std::vector<int>, std::greater<int>>;
    using threads_array =  std::array<std::unique_ptr<Thread>, MAX_THREAD_NUM>;

private:

    priority_queue _free_idx;
    threads_array _threads;
    std::vector<int> _priority_time;

    Thread *_curr_thread{};

    int _next_free_idx = 0;
    int _priority_size{};
    int _total_quantums = 0;

    /*
     * constructor of ThreadManager : default
     */
    ThreadManager() = default;


public:
    std::list<int> ready;

    /*
     * This function returning the instance of the singleton class
     */
    static ThreadManager &getInstance() noexcept
    {
        static ThreadManager instance;
        return instance;
    }

    /*
     * This function set the size of priority_time array and copy its content to the class
     * priority_time - array of time im micro-seconds for each priority
     * size - the size of the given array
     * Return value: On success, return 0.
     * On failure, return -1.
     */
    int set_priority_time(const int *priority_time, int size);


    /*
     * This function get the quantum for the thread with the given id.
     * id - the id of the thread
     * Return value: On success, return the quantum.
     * On failure, return -1.
     */
    int get_time(int id);

    /*
     * This function update the next free id from free_idx, otherwise return the curr_id
     */
    void update_next_idx();

    /*
     * This function add the thread to the threads array
     */
    int add_thread(std::unique_ptr<Thread> thread);

    /*
     * get the id of the next thread to run : the next in the ready list
     */
    int get_next_ready();

    /*
     * get the thread with the given id
     */
    Thread *get_thread(int id);

    /*
     * get the next free id, in case the free_idx is empty, returning the curr id
     */
    int get_next_idx()
    {
        update_next_idx();
        return _next_free_idx;
    }

    /*
     * remove the given id from the ready list
     */
    void remove_from_ready(int id)
    {
        ready.remove(id);
    }

    /*
     * remove the thread that have the given id from threads array
     */
    void remove_thread(int id)
    {
        _threads[id] = nullptr;
        _free_idx.push(id);
    }

    /*
     * set the current thread to the thread with the given id
     */
    void set_curr_thread(int id);

    /*
     * increase the total quantum by 1
     */
    void inc_total_quantums()
    {
        _total_quantums++;
    }

    /*
     * get the current running thread
     */
    Thread *get_curr_thread()
    {
        return _curr_thread;
    }

    /*
     * get the current running thread id
     */
    int get_curr_id() const
    {
        return _curr_thread->id;
    }

    /*
     * set the next thread to run
     */
    void set_next()
    {
        auto next = get_next_ready();
        set_curr_thread(next);
    }

    /*
     * get the total quantum num
     */
    int get_total_quantums() const
    {
        return _total_quantums;
    }

    /*
     *  Check if the given id is valid.
     */
    static bool is_id_valid(int id)
    {
        return id < MAX_THREAD_NUM and 0 <= id;
    }

    /*
     * Check if the given priority is valid.
     */
    bool is_priority_valid(int priority) const
    {
        return priority < _priority_size and 0 <= priority;
    }


};


#endif //OS_EX2_THREADMANAGER_H
