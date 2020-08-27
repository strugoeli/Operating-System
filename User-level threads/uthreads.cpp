/*
 * User-Level Threads Library (uthreads) implementation
 *
 */


#include "ThreadManager.h"
#include <sys/time.h>
#include <iostream>
#include <setjmp.h>
#include "uthreads.h"


//------------------------- MACROS -------------------------//

#define SUCCESS 0
#define FAILURE (-1)

#define READY 0
#define RUNNING 1
#define BLOCKED 2

static const char *const INVALID_PRIORITY = "Invalid priority";
static const char *const TIMER_ERR = "setitimer error.";
static const char *const THREAD_NOT_FOUND = "Thread does not exists";
static const char *const ADD_THREAD_ERR = "Cannot add thread";
static const char *const FAILD_TO_ADD = "Failed to add";



static const int SAVE_MASK = 1;

static const int SEC_FACTOR = 1000000;

static const bool BLOCK = true;

static const bool UNBLOCK = false;

//-------------------- GLOBAL VARIABLES --------------------//



struct sigaction sa{};
static ThreadManager &manager = ThreadManager::getInstance();
static const int MAIN_ID = 0;
static const int MAIN_PRIORITY = 0;
sigjmp_buf env{};
sigset_t set{};
struct itimerval timer{};


//---------------------Helpers-----------------------------//

/*
 * handle library errors
 */
void lib_error(const std::string &msg)
{
    std::cerr << "thread library error: " + msg << "\n";

}

/*
 * handle system call errors
 */
void sys_error(const std::string &msg)
{
    std::cerr << "system error: " + msg << "\n";
    exit(FAILURE);

}

/*
 * set the timer by the current running thread priority
 */
void set_time()
{

    auto time = manager.get_time(manager.get_curr_id());

    timer.it_value.tv_sec = time / SEC_FACTOR;
    timer.it_value.tv_usec = time % SEC_FACTOR;;


    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = 0;

    // Start a virtual timer. It counts down whenever this process is executing.
    if (setitimer(ITIMER_VIRTUAL, &timer, nullptr))
    {
        sys_error(TIMER_ERR);
    }
}

/*
 * Switch to block state
 */
void switch_block_state(bool block)
{
    auto cmd = block ? SIG_BLOCK : SIG_UNBLOCK;
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigaddset(&set, SIGVTALRM);
    sigprocmask(cmd, &set, nullptr);
}


/*
 * Signal handler
 */
void sig_handler(int sig = 0)
{
    switch_block_state(BLOCK);
    auto thread = manager.get_curr_thread();
    auto ret = sigsetjmp(thread->env, SAVE_MASK);

    if (ret == 1)
    {
        return;
    }

    if (manager.ready.empty())
    {
        thread->num_of_quantums++;
        manager.inc_total_quantums();
    }
    else
    {
        if (sig == SIGVTALRM)
        {
            thread->state = READY;
            manager.ready.push_back(thread->id);
        }
        manager.set_next();
    }

    set_time();
    switch_block_state(UNBLOCK);
    siglongjmp(manager.get_curr_thread()->env, SAVE_MASK);
}



//--------------Functions-----------------------------------------

/*
 * Description: This function initializes the thread library.
 * You may assume that this function is called before any other thread library
 * function, and that it is called exactly once. The input to the function is
 * an array of the length of a quantum in micro-seconds for each priority.
 * It is an error to call this function with an array containing non-positive integer.
 * size - is the size of the array.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_init(int *quantum_usecs, int size)
{
    try
    {

        switch_block_state(BLOCK);

        if (manager.set_priority_time(quantum_usecs, size) == FAILURE)
        {
            lib_error(INVALID_PRIORITY);
            return FAILURE;
        }

        std::unique_ptr<Thread> new_thread(new Thread(MAIN_ID, RUNNING, MAIN_PRIORITY, nullptr));

        int add_res = manager.add_thread(std::move(new_thread));

        if (add_res == FAILURE)
        {
            lib_error(FAILD_TO_ADD);
            return FAILURE;
        }

        manager.set_curr_thread(MAIN_ID);

        sa.sa_handler = &sig_handler;
        sigsetjmp(env, SAVE_MASK);

        if (sigaction(SIGVTALRM, &sa, nullptr) < 0)
        {
            sys_error("sigaction error");
        }

        set_time();
        switch_block_state(UNBLOCK);


        return SUCCESS;
    }
    catch (std::bad_alloc &e)
    {
        sys_error(e.what());
    }

}

/*
 * Description: This function creates a new thread, whose entry point is the
 * function f with the signature void f(void). The thread is added to the end
 * of the READY threads list. The uthread_spawn function should fail if it
 * would cause the number of concurrent threads to exceed the limit
 * (MAX_THREAD_NUM). Each thread should be allocated with a stack of size
 * STACK_SIZE bytes.
 * priority - The priority of the new thread.
 * Return value: On success, return the ID of the created thread.
 * On failure, return -1.
*/
int uthread_spawn(void (*f)(void), int priority)
{
    try
    {
        switch_block_state(BLOCK);
        std::unique_ptr<Thread> new_thread(new Thread(manager.get_next_idx(), READY, priority, f));

        int add_res = manager.add_thread(std::move(new_thread));
        if (add_res == FAILURE)
        {
            lib_error(ADD_THREAD_ERR);

            return FAILURE;
        }
        switch_block_state(UNBLOCK);


        return add_res;
    }

    catch (std::bad_alloc &e)
    {
        sys_error(e.what());
    }
}


/*
 * Description: This function changes the priority of the thread with ID tid.
 * If this is the current running thread, the effect should take place only the
 * next time the thread gets scheduled.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_change_priority(int tid, int priority)
{

    auto thread = manager.get_thread(tid);
    if (!thread or !manager.is_priority_valid(priority))
    {
        return FAILURE;
    }
    thread->priority = priority;

    return SUCCESS;
}


/*
 * Description: This function terminates the thread with ID tid and deletes
 * it from all relevant control structures. All the resources allocated by
 * the library for this thread should be released. If no thread with ID tid
 * exists it is considered an error. Terminating the main thread
 * (tid == 0) will result in the termination of the entire process using
 * exit(0) [after releasing the assigned library memory].
 * Return value: The function returns 0 if the thread was successfully
 * terminated and -1 otherwise. If a thread terminates itself or the main
 * thread is terminated, the function does not return.
*/
int uthread_terminate(int tid)
{
    switch_block_state(BLOCK);
    if (tid == 0)
    {
        exit(SUCCESS);
    }


    auto thread = manager.get_thread(tid);
    if (!thread)
    {
        lib_error(THREAD_NOT_FOUND);
        return FAILURE;
    }
    auto state = thread->state;

    if (state == READY)
    {
        manager.remove_from_ready(tid);

    }
    else if (state == RUNNING)
    {
        manager.set_next();

    }

    manager.remove_thread(tid);
    if (state == RUNNING)
    {
        set_time();
        siglongjmp(manager.get_curr_thread()->env, SAVE_MASK);

    }
    switch_block_state(UNBLOCK);


    return SUCCESS;
}

//
/*
 * Description: This function blocks the thread with ID tid. The thread may
 * be resumed later using uthread_resume. If no thread with ID tid exists it
 * is considered as an error. In addition, it is an error to try blocking the
 * main thread (tid == 0). If a thread blocks itself, a scheduling decision
 * should be made. Blocking a thread in BLOCKED state has no
 * effect and is not considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_block(int tid)
{
    switch_block_state(BLOCK);

    if (tid == MAIN_ID)
    {
        lib_error("Cannot block main thread");

        return FAILURE;
    }

    auto thread = manager.get_thread(tid);
    if (!thread)
    {
        lib_error(THREAD_NOT_FOUND);

        return FAILURE;
    }
    auto curr_state = thread->state;
    thread->state = BLOCKED;

    if (curr_state == RUNNING)
    {
        switch_block_state(UNBLOCK);

        sig_handler();
    }
    else if (curr_state == READY)
    {
        manager.remove_from_ready(tid);
    }
    switch_block_state(UNBLOCK);


    return SUCCESS;


}


/*
 * Description: This function resumes a blocked thread with ID tid and moves
 * it to the READY state. Resuming a thread in a RUNNING or READY state
 * has no effect and is not considered as an error. If no thread with
 * ID tid exists it is considered an error.
 * Return value: On success, return 0. On failure, return -1.
*/
int uthread_resume(int tid)
{
    auto thread = manager.get_thread(tid);
    if (!thread)
    {
        lib_error(THREAD_NOT_FOUND);
        return FAILURE;
    }
    if (thread->state == BLOCKED)
    {
        thread->state = READY;
        manager.ready.push_back(thread->id);
    }
    return SUCCESS;
}


/*
 * Description: This function returns the thread ID of the calling thread.
 * Return value: The ID of the calling thread.
*/
int uthread_get_tid()
{
    auto curr_thread = manager.get_curr_thread();
    if (!curr_thread)
    {
        return FAILURE;
    }

    return curr_thread->id;

}


/*
 * Description: This function returns the total number of quantums since
 * the library was initialized, including the current quantum.
 * Right after the call to uthread_init, the value should be 1.
 * Each time a new quantum starts, regardless of the reason, this number
 * should be increased by 1.
 * Return value: The total number of quantums.
*/
int uthread_get_total_quantums()
{
    return manager.get_total_quantums();
}


/*
 * Description: This function returns the number of quantums the thread with
 * ID tid was in RUNNING state. On the first time a thread runs, the function
 * should return 1. Every additional quantum that the thread starts should
 * increase this value by 1 (so if the thread with ID tid is in RUNNING state
 * when this function is called, include also the current quantum). If no
 * thread with ID tid exists it is considered an error.
 * Return value: On success, return the number of quantums of the thread with ID tid.
 * 			     On failure, return -1.
*/
int uthread_get_quantums(int tid)
{
    auto thread = manager.get_thread(tid);
    if (!thread)
    {
        lib_error(THREAD_NOT_FOUND);
        return FAILURE;
    }
    return thread->num_of_quantums;

}



