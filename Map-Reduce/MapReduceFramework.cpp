#include <algorithm>
#include <atomic>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <iostream>
#include <bitset>

/* atomic multivar variable*/
static const uint64_t ONES_BIT = 0x7FFFFFFF;
// 001111...11 (60 times 1)
static const uint64_t ONES_STAGE = 0x3FFFFFFFFFFFFFFF;
static const uint64_t STAGE_LOCATION = 62u;
static const uint64_t TOTAL_LOCATION = 31u;
static const uint64_t BIT_62 = 0x4000000000000000;


#define ERROR_INIT_MUTEX "error in pthread_mutex_init"
#define SYSTEM_ERROR "system error: "
#define ERROR_LOCK_MUTEX  "error in pthread_mutex_lock"
#define ERROR_UNLOCK_MUTEX "error in pthread_mutex_unlock"
#define ERROR_BAD_ALLOC "error bad alloc"


struct ThreadContext;
struct JobContext;


/*
 * struct of job context
 */
typedef struct JobContext
{
    pthread_t *jobThreads;

    int numThreads;
    const MapReduceClient &client;
    const InputVec &inputVec;

    //for helping in map Phase ("index in input pairs")
    std::atomic<int> *inputCounter;
    std::atomic<int> *numFinishMapPairs;
    std::atomic<int> *shuffleCounter;
    std::atomic<int> *reduceKeyCounter;
    std::atomic<int> *mapThreadCounter;


    std::vector<K2 *> uniqueKeys;
    IntermediateMap intermediateMap;
    OutputVec &outputVec;
    std::vector<ThreadContext *> *threadsContext;


    //mutex
    pthread_mutex_t *outMutex;
    pthread_mutex_t *reduceMutex;


    std::vector<pthread_mutex_t *> *threadsMutex;

    Barrier *barrier;
    bool joinFlag;
    std::atomic<uint64_t> *atomicState;

} JobContext;


/*
 * struct of thread context
 */
typedef struct ThreadContext
{
    int tId;

    JobContext *jobContext;
    std::vector<IntermediatePair> *interPairs;

    ThreadContext() = default;

    ThreadContext(int threadID, JobContext *jobContext, std::vector<IntermediatePair> *interPairs)
            : tId(threadID), jobContext(jobContext), interPairs(interPairs)
    {}


} ThreadContext;


/**
 * initial all value that not the stage and calculate the percentage (according to stage)
 * maybe change to only update the proccessed keys?
 * @param jobCon
 */
void updateState(JobContext *jobContext, stage_t stage)
{

    if (stage == MAP_STAGE)
    {
        jobContext->atomicState->store((ONES_STAGE & MAP_STAGE) << STAGE_LOCATION);
        int inputSize = jobContext->inputVec.size();
        uint64_t total = (*jobContext->atomicState) + (ONES_BIT & (uint64_t) (inputSize) << TOTAL_LOCATION);
        jobContext->atomicState->store(total);

    }
    else if (stage == SHUFFLE_STAGE)
    {
        jobContext->atomicState->store((ONES_STAGE & SHUFFLE_STAGE) << STAGE_LOCATION);

        auto jobAtomic = (uint64_t) (jobContext->atomicState->load());
        auto jobStage = (stage_t) (jobAtomic >> STAGE_LOCATION);
        *jobContext->atomicState = (ONES_STAGE & jobStage) << STAGE_LOCATION;

        *jobContext->atomicState += (ONES_BIT & (uint64_t) (jobContext->numFinishMapPairs->load())) << TOTAL_LOCATION;
        *jobContext->atomicState += (uint64_t) (*jobContext->shuffleCounter);
    }
    else if (stage == REDUCE_STAGE)
    {
        int numKeys = (jobContext->uniqueKeys.size());
        uint64_t bitStage = (~ONES_STAGE);
        uint64_t temp = ((numKeys & ONES_BIT) << TOTAL_LOCATION);
        jobContext->atomicState->store((bitStage + temp));

    }
}


/*
 * This function lock the given mutex
 * @param mutex
 */
void lockMutex(pthread_mutex_t *mutex, JobContext *jobContext)
{
    if (pthread_mutex_lock(mutex))
    {
        std::cerr << SYSTEM_ERROR << ERROR_LOCK_MUTEX << std::endl;
        closeJobHandle(jobContext);
        exit(EXIT_FAILURE);
    }
}

/*
 * This function unlock the given mutex
 * @param mutex
 */
void unlockMutex(pthread_mutex_t *mutex, JobContext *jobContext)
{
    if (pthread_mutex_unlock(mutex))
    {
        std::cerr << SYSTEM_ERROR << ERROR_UNLOCK_MUTEX << std::endl;
        closeJobHandle(jobContext);

        exit(EXIT_FAILURE);
    }
}

/*
 * This function destroy the given mutex
 * @param mutex
 */
void destroyMutex(pthread_mutex_t *mutex)
{
    if (pthread_mutex_destroy(mutex))
    {
        std::cerr << SYSTEM_ERROR << ERROR_UNLOCK_MUTEX << std::endl;

        exit(EXIT_FAILURE);
    }
}

/*
 * In this phase each thread reads pairs of (k1,v1) from the input vector and calls the map function on
 * each of them.
 * @param arg
 */
void runMapPhase(ThreadContext *tc)
{

    JobContext *jobContext = tc->jobContext;


    auto inputSize = (int) jobContext->inputVec.size();
    auto oldValue = (*jobContext->inputCounter)++;

    while (oldValue < inputSize)
    {

        auto pair = jobContext->inputVec[oldValue];
        tc->jobContext->client.map(pair.first, pair.second, tc);
        jobContext->atomicState->fetch_add(1);
        oldValue = (*jobContext->inputCounter)++;

    }
    (*jobContext->mapThreadCounter)++;

}

void fillInterMap(JobContext *jobCon, IntermediateMap *intermediateMap, stage_t stage)
{
    for (int i = 0; i < jobCon->numThreads - 1; ++i)
    {
        pthread_mutex_t *t_mutex = (*jobCon->threadsMutex)[i];
        if (stage == MAP_STAGE)
        {
            lockMutex(t_mutex, jobCon);
        }

        ThreadContext *t_context = (*jobCon->threadsContext)[i];
        while (!(*jobCon->threadsContext)[i]->interPairs->empty())
        {
            auto pair = &t_context->interPairs->back();
            t_context->interPairs->pop_back();

            if ((*intermediateMap).find(pair->first) == (*intermediateMap).end())
            {
                (*intermediateMap)[pair->first] = std::vector<V2 *>();
                jobCon->uniqueKeys.push_back(pair->first);
            }
            (*intermediateMap)[pair->first].push_back(pair->second);
            (*jobCon->shuffleCounter)++;
            if (stage == SHUFFLE_STAGE)
            {
                jobCon->atomicState->fetch_add(1);
            }
        }

        if (stage == MAP_STAGE)
        {
            unlockMutex(t_mutex, jobCon);
        }

    }

}

/*
 * This function run the ShufflePhase
 * @param tc the context of the shuffle thread
 */
void runShufflePhase(ThreadContext *tc)
{

    JobContext *jobCon = tc->jobContext;
    IntermediateMap *intermediateMap = &tc->jobContext->intermediateMap;
    auto n = jobCon->numThreads;

    while (jobCon->mapThreadCounter->load() < n - 1)
    {
        if (jobCon->numFinishMapPairs->load())
        {
            fillInterMap(jobCon, intermediateMap, MAP_STAGE);
        }
    }

    updateState(jobCon, SHUFFLE_STAGE);
    fillInterMap(jobCon, intermediateMap, SHUFFLE_STAGE);
    updateState(jobCon, REDUCE_STAGE);


}

/*
 * This function run the ReducePhase
 * @param tc the context of the shuffle thread
 */
void runReducePhase(void *context)
{

    auto *tc = (ThreadContext *) context;
    auto jobCon = tc->jobContext;

    lockMutex(jobCon->reduceMutex, jobCon);

    int numKeys = jobCon->uniqueKeys.size();
    int oldValue = *jobCon->reduceKeyCounter;
    ++(*jobCon->reduceKeyCounter);
    while (oldValue < numKeys)
    {

        K2 *key = jobCon->uniqueKeys[oldValue];
        jobCon->client.reduce(key, jobCon->intermediateMap[key], jobCon);
        oldValue = *jobCon->reduceKeyCounter;
        ++(*jobCon->reduceKeyCounter);

    }
    unlockMutex(jobCon->reduceMutex, jobCon);


}


/*
 * This function running the MapReduce algorithm
 * @param arg the context of the thread
 */
void *runJob(void *arg)
{
    auto *tc = (ThreadContext *) arg;

    if (tc->tId < tc->jobContext->numThreads - 1)
    {
        runMapPhase(tc);
    }
    else
    {
        runShufflePhase(tc);
    }
    tc->jobContext->barrier->barrier();

    runReducePhase(tc);
    return nullptr;
}

/*
 * This function produces a (K2*,V2*) pair
 * @param key
 * @param value
 * @param context
 */
void emit2(K2 *key, V2 *value, void *context)
{

    auto tc = static_cast<ThreadContext *> (context);
    pthread_mutex_t *outMutex = (*tc->jobContext->threadsMutex)[tc->tId];
    lockMutex(outMutex, tc->jobContext);
    tc->interPairs->push_back(std::make_pair(key, value));
    (*tc->jobContext->numFinishMapPairs)++;
    unlockMutex(outMutex, tc->jobContext);
}

/*
 * This function produces a (K3*,V3*) pair
 * @param key
 * @param value
 * @param context
 */
void emit3(K3 *key, V3 *value, void *context)
{
    auto jobCon = static_cast<JobContext *> (context);
    pthread_mutex_t *outMutex = jobCon->outMutex;
    lockMutex(outMutex, jobCon);
    jobCon->outputVec.push_back(OutputPair(key, value));
    jobCon->atomicState->store((*jobCon->atomicState) + uint64_t(1));

    unlockMutex(outMutex, jobCon);

}


/*
 * This function starts running the MapReduce algorithm (with
 * several threads) and returns a JobHandle.
 * @param client - The implementation of MapReduceClient class, in other words the task that the
 * framework should run.
 * @param inputVec - the input elements
 * @param outputVec - a vector to which the output
 * elements will be added before returning
 * @param multiThreadLevel - the number of worker threads to be used for running the algorithm.
 * You can assume this argument is valid
 * @return a JobHandle representing the job
 */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    if (inputVec.empty() or !multiThreadLevel)
    {
        exit(EXIT_FAILURE);
    }
    try
    {

        auto *threads = new pthread_t[multiThreadLevel];


        auto *pairsFinishedMap = new std::atomic<int>(0);
        auto *threadsFinishedShuffle = new std::atomic<int>(0);
        auto *indexInputPair = new std::atomic<int>(0);
        auto *numReducedKeys = new std::atomic<int>(0);
        auto *mapThreadCounter = new std::atomic<int>(0);
        auto *atomicState = new std::atomic<uint64_t>(0);

        // first stage is
        uint64_t totalSize = BIT_62 + (*atomicState) + ((ONES_BIT & inputVec.size()) << TOTAL_LOCATION);

        atomicState->store(totalSize);

        std::vector<K2 *> uniqueKeys;
        IntermediateMap intermediateMap;

        auto *outMutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
        auto *reduceMutex = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);

        auto *threadsMutex = new std::vector<pthread_mutex_t *>(multiThreadLevel - 1);
        for (int j = 0; j < multiThreadLevel - 1; ++j)
        {
            (*threadsMutex)[j] = new pthread_mutex_t(PTHREAD_MUTEX_INITIALIZER);
        }
        auto *barrier = new Barrier(multiThreadLevel);
        auto *threadsContext = new std::vector<ThreadContext *>(multiThreadLevel);
        auto *jobContext = new JobContext{threads, multiThreadLevel, client, inputVec,
                                          indexInputPair, pairsFinishedMap, threadsFinishedShuffle, numReducedKeys,
                                          mapThreadCounter, uniqueKeys, intermediateMap, outputVec, threadsContext,
                                          outMutex, reduceMutex, threadsMutex, barrier, false, atomicState};

        for (int i = 0; i < multiThreadLevel; ++i)
        {
            auto *t_context = new ThreadContext;
            t_context->tId = i;
            t_context->jobContext = jobContext;
            t_context->interPairs = (i == multiThreadLevel - 1) ? nullptr : new std::vector<IntermediatePair>;
            pthread_create(&threads[i], nullptr, runJob, t_context);
            (*jobContext->threadsContext)[i] = t_context;
        }
        return (JobHandle) jobContext;
    }
    catch (std::bad_alloc &e)
    {
        std::cerr << SYSTEM_ERROR << ERROR_BAD_ALLOC << std::endl;

        exit(EXIT_FAILURE);
    }

};


/*
 * a function gets the job handle returned by startMapReduceFramework and
 * waits until it is finished.
 * @param job
 */
void waitForJob(JobHandle job)
{

    {
        auto jobContext = static_cast<JobContext *> (job);
        if (jobContext->joinFlag)
        {
            return;
        }

        for (int i = 0; i < jobContext->numThreads; ++i)
        {
            // if success return 0
            if (pthread_join(jobContext->jobThreads[i], nullptr))
            {
                std::cerr << SYSTEM_ERROR << "pthread join failed " << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        jobContext->joinFlag = true;


    }
}


/*
 * this function gets a job handle and updates the state of the job into the given
 * JobState struct.
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState *state)
{
    auto jobContext = static_cast<JobContext *> (job);
    auto jobAtomic = (uint64_t) (*jobContext->atomicState);
    auto jobStage = (stage_t) (jobAtomic >> STAGE_LOCATION);
    int jobTotal = (int) ((ONES_STAGE & jobAtomic) >> TOTAL_LOCATION);
    int jobProc = (int) (jobAtomic & ONES_BIT);
    float statePercent = (jobTotal == 0) ? 0 : ((float) jobProc / (float) jobTotal) * 100;
    *state = {jobStage, statePercent};

};


/*
 * Releasing all resources of a job. You should prevent releasing resources
 * before the job is finished. After this function is called the job handle will be invalid.
 * @param job
 */
void closeJobHandle(JobHandle job)
{

    waitForJob(job);
    auto jobCon = (JobContext *) job;
    for (auto mutex: *jobCon->threadsMutex)
    {
        if (mutex != nullptr)
        {
            destroyMutex(mutex);
            delete (mutex);
        }
    }
    destroyMutex(jobCon->reduceMutex);
    destroyMutex(jobCon->outMutex);


    for (auto tc : *jobCon->threadsContext)
    {
        delete tc->interPairs;
        delete (tc);
    }
    delete (jobCon->threadsContext);
    delete (jobCon->threadsMutex);

    delete (jobCon->reduceMutex);
    delete (jobCon->outMutex);

    delete jobCon->atomicState;
    delete jobCon->shuffleCounter;
    delete jobCon->mapThreadCounter;
    delete jobCon->numFinishMapPairs;
    delete jobCon->reduceKeyCounter;
    delete jobCon->inputCounter;
    delete jobCon->barrier;
    delete[] jobCon->jobThreads;
    jobCon->intermediateMap.clear();
    jobCon->uniqueKeys.clear();
    delete (jobCon);

}