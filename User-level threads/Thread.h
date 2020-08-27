/*
 * Class representing the instances of threads
 */

#ifndef EX2_OS_THREAD_H
#define EX2_OS_THREAD_H

#include <setjmp.h>
#include <signal.h>
#include "uthreads.h"

//------------------------- MACROS -------------------------//

#define RUNNING 1


/*
 * The Thread class
 */
class Thread
{
public:

    int id = 0;
    int state = 0;
    int priority = 0;
    int num_of_quantums = 0;

    char stack[STACK_SIZE]{};
    sigjmp_buf env{};

    typedef unsigned long address_t;

    /*
     * constructor
     */
    Thread(int tId, int tState, int tPriority, void (*f)());

    /*
     * operator <
     */
    bool operator<(const Thread &other) const
    {
        return id < other.id;
    }

    /*
     * operator >=
     */
    bool operator>=(const Thread &other) const
    {
        return !(*this < other);
    }

    /*
     * operator ==
     */
    bool operator==(const Thread &other) const
    {
        return id == other.id;
    }


};

#endif //EX2_OS_THREAD_H
