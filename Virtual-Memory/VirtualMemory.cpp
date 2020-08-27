#include <cmath>
#include <algorithm>
#include "VirtualMemory.h"
#include "PhysicalMemory.h"

#define EMPTY_SIGN 0

using std::abs;
using std::min;


void clearTable(uint64_t frameIndex)
{
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
    {
        PMwrite(frameIndex * PAGE_SIZE + i, 0);
    }
}

void VMinitialize()
{
    clearTable(0);
}

/*
 * Checks if in the given frame address there is empty table
 */
bool isEmptyTable(word_t frameNum)
{
    for (int i = 0; i < PAGE_SIZE; ++i)
    {
        word_t row = 0;
        PMread(frameNum * PAGE_SIZE + i, &row);
        if (row != EMPTY_SIGN)
        {
            return false;
        }
    }
    return true;
}

uint64_t getCyclicDistance(long long pageNum, long long indexP)
{
    auto diff = abs((int) pageNum - indexP);
    return min((const int) NUM_PAGES - diff, diff);
}

/*
 * This function computing the cyclical Distance, to handle case 3
 */
void updateDistParam(word_t currFrame, uint64_t pageNum, long long *maxDist, int indexP, uint64_t *distPage, word_t parent,
                word_t *distFrame, word_t *distParent, int currIdx)
{
    auto cyclicalDis = getCyclicDistance(pageNum, indexP);
    if (currFrame != 0 and cyclicalDis > (uint64_t) *maxDist)
    {
        *maxDist = cyclicalDis;
        *distFrame = currFrame;
        *distPage = indexP;
        *distParent = parent * PAGE_SIZE + currIdx;
    }
}

/*
 * This function remove the given frame address from the table at the given page index
 */
void removeParentAddress(word_t parent, int indexP)
{
    uint64_t currOffset = (unsigned) indexP & (unsigned) (PAGE_SIZE - 1);
    PMwrite((parent * PAGE_SIZE) + currOffset, EMPTY_SIGN);
}


/*
 * Searching empty table, in success return the num table, otherwise return 0
 */
word_t
dfs(word_t *maxFrame, const word_t *prevFame, word_t *distFrame,
    long long *maxDist, uint64_t pageNum, uint64_t *distPage, word_t *distParent, int currIdx = 0, word_t currFrame = 0,
    word_t parent = -1, uint64_t currPagePath = 0, int depth = 0)
{

    *maxFrame = (currFrame > *maxFrame) ? currFrame : *maxFrame;
    if (depth == TABLES_DEPTH)
    {
        updateDistParam(currFrame, pageNum, maxDist, currPagePath, distPage, parent, distFrame, distParent, currIdx);

        return 0;
    }

    if (currFrame != 0 and *prevFame != currFrame and isEmptyTable(currFrame))
    {
        removeParentAddress(parent, currPagePath);
        return currFrame;
    }


    uint64_t currPAddress = 0;
    word_t nextFrame, res;
    for (int i = 0; i < PAGE_SIZE; ++i)
    {
        nextFrame = 0;
        currPAddress = currFrame * PAGE_SIZE + i;
        PMread(currPAddress, &nextFrame);

        if (nextFrame == EMPTY_SIGN)
        {
            continue;
        }

        auto p = ((unsigned) currPagePath << (unsigned) OFFSET_WIDTH) + i;
        res = dfs(maxFrame, prevFame, distFrame, maxDist, pageNum, distPage,
                  distParent, i, nextFrame, currFrame, p, depth + 1);
        if (res)
        {
            return res;
        }
    }
    return 0;
}

/*
 * This function fill the pages array with pages addresses
 */
void fillPages(uint64_t pageNum, uint64_t *pages)
{
    for (int i = (TABLES_DEPTH - 1); i >= 0; --i)
    {
        pages[i] = pageNum & (unsigned) (PAGE_SIZE - 1);
        pageNum = pageNum >> (unsigned) OFFSET_WIDTH;
    }
}


/*
 * This function find the address to the next available frame and return its address
 */
word_t getNextFrame(word_t *prevFrame, uint64_t pageNum)
{
    word_t maxFrame, frameToEvict, fToEvictParent;
    maxFrame = frameToEvict = fToEvictParent = 0;
    uint64_t evictedPageIndex = 0;
    long long dist = 0;

    auto nextFrame = dfs(&maxFrame, prevFrame, &frameToEvict, &dist, pageNum, &evictedPageIndex, &fToEvictParent);
    if (nextFrame != EMPTY_SIGN)
    {
        return nextFrame;
    }
    if (maxFrame + 1 < NUM_FRAMES)
    {
        return maxFrame + 1;
    }

    PMevict((uint64_t) frameToEvict, (uint64_t) evictedPageIndex);
    PMwrite(fToEvictParent, EMPTY_SIGN);

    return frameToEvict;
}

/*
 * In case of page fault, it either cleans the table or restores it from the disk
 */
void prepareNextFrame(word_t frame, uint64_t pageNum, int depth)
{
    if (depth < TABLES_DEPTH - 1)
    {
        clearTable(frame);
    }
    else
    {
        PMrestore(frame, pageNum);
    }
}

/*
 * This function get the address of the frame in PM
 */
uint64_t getFrameAddr(uint64_t virtualAddress)
{

    uint64_t pageNum = virtualAddress >> (unsigned) OFFSET_WIDTH;
    uint64_t pages[TABLES_DEPTH];
    fillPages(pageNum, pages);
    word_t currFrame = 0, nextFrame = 0;

    for (int j = 0; j < TABLES_DEPTH; ++j)
    {
        PMread(currFrame * PAGE_SIZE + pages[j], &nextFrame);
        if (nextFrame == EMPTY_SIGN)
        {
            nextFrame = getNextFrame(&currFrame, pageNum);
            prepareNextFrame(nextFrame, pageNum, j);
            PMwrite(currFrame * PAGE_SIZE + pages[j], nextFrame);
        }

        currFrame = nextFrame;
        nextFrame = 0;
    }
    return currFrame;
}


int VMread(uint64_t virtualAddress, word_t *value)
{
    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    uint64_t offset = virtualAddress & (unsigned) (PAGE_SIZE - 1);
    uint64_t nextVal = getFrameAddr(virtualAddress);
    PMread(nextVal * PAGE_SIZE + offset, value);
    return 1;
}


int VMwrite(uint64_t virtualAddress, word_t value)
{

    if (virtualAddress >= VIRTUAL_MEMORY_SIZE)
    {
        return 0;
    }
    uint64_t offset = virtualAddress & (unsigned) (PAGE_SIZE - 1);
    uint64_t nextVal = getFrameAddr(virtualAddress);
    PMwrite(nextVal * PAGE_SIZE + offset, value);
    return 1;
}







