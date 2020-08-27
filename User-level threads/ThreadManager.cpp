//
// Created by strugo on 07/05/2020.
//
#include "ThreadManager.h"


int ThreadManager::get_time(int id)
{

    if (!is_id_valid(id))
    {
        return FAILURE;
    }
    auto priority = _threads[id]->priority;
    return _priority_time[priority];
}

void ThreadManager::update_next_idx()
{
    if (!_free_idx.empty() and _threads[0])
    {
        _next_free_idx = _free_idx.top();
        _free_idx.pop();
    }
}

int ThreadManager::add_thread(std::unique_ptr<Thread> thread)
{
    if (is_id_valid(_next_free_idx) and !_threads[_next_free_idx])
    {
        int curr_idx = _next_free_idx;

        _threads[curr_idx] = std::move(thread);

        if (_free_idx.empty() and is_id_valid(curr_idx + 1) and !_threads[curr_idx + 1])
        {
            _free_idx.push(curr_idx + 1);
        }

        if (_threads[curr_idx].get()->state == READY)
        {
            ready.push_back(curr_idx);

        }

        return curr_idx;
    }
    return FAILURE;

}

void ThreadManager::set_curr_thread(int id)
{
    _curr_thread = _threads[id].get();
    _threads[id]->state = RUNNING;

    _curr_thread->num_of_quantums++;

    _total_quantums++;

}

int ThreadManager::get_next_ready()
{
    if (ready.empty())
    {
        exit(0);
    }
    auto next = ready.front();
    ready.pop_front();
    return next;
}




Thread *ThreadManager::get_thread(int id)
{
    if (!is_id_valid(id))
    {
        return nullptr;
    }
    return _threads[id].get();
}

int ThreadManager::set_priority_time(const int *priority_time, int size)
{
    if (priority_time == nullptr or size < 1)
    {
        return FAILURE;
    }

    _priority_size = size;
    for (int i = 0; i < size; i++)
    {

        if (priority_time[i] < 1)
        {
            return FAILURE;
        }
        _priority_time.push_back(priority_time[i]);
    }
    return SUCCESS;
}
