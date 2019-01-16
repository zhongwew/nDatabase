
#include "ix.h"

bool compareVarChar(bool inclusive, const void* first, int firstLength, const void* second, int secondLength);
void* pairAnalysis(const Attribute &attribute, const void *key, const RID &rid, int& dataLength);

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}


IndexManager::IndexManager()
{
    this->pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    if(pfm->createFile(fileName) != 0)
        return -1;
    return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    if(pfm->destroyFile(fileName) != 0)
        return -1;
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if(pfm->openFile(fileName, ixfileHandle.indexFileHandle))
    {
        return -1;
    }

    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    if(ixfileHandle.indexFileHandle.closeFile())
    {
        return -1;
    }
    else
    {
        return 0;
    }
}

/**
 * Function: Compact the key and rid into void* buffer, Assign dataLength.
 */
void* pairAnalysis(const Attribute &attribute, const void *key, const RID &rid, int& dataLength)
{

    // 1. get key length
    int keyLength = 0;
    // 2. allocate buffer for pairData
    void* buffer = malloc(PAGE_SIZE);
    void* data = buffer;
    memset(buffer, 0, PAGE_SIZE);


    // 3. copy the key data to buffer
    int offset = 0; //used to track offset in pair data
    if(attribute.type == TypeVarChar)
    {
        // copy the key to the buffer
        memcpy(&keyLength, key, sizeof(unsigned));
        // cout << "keyLength: " << keyLength << endl;
        memcpy(buffer, (char*)key, keyLength+sizeof(unsigned));
        offset += (sizeof(unsigned) + keyLength);
        buffer = (char*)buffer + offset;
    }
    else
    {
        // copyt the int/read key value to buffer
        memcpy(buffer, key, sizeof(unsigned));
        offset += sizeof(unsigned);
        buffer = (char*)buffer + offset;     
    }

    // 4. copy the rid data to buffer
    memcpy(buffer, &rid, sizeof(RID)); 
    offset += sizeof(RID);
    // cout <<"offset: "<< offset << endl;

    // 5. set dataLength
    dataLength = offset;
    // memcpy(dataLength, &offset, sizeof(int));

    // cout << "dataLength" << &dataLength << endl;

    // 6. return void* data
    return data;

}

/**
 * return: true if first <= second, false if first > second
 */
bool compareVarChar(bool inclusive, const void* first, int firstLength, const void* second, int secondLength)
{
    int maxVarCharLength = 0;
    // get the maxLength of two varchar key
    if(firstLength >= secondLength)
    {
        maxVarCharLength = firstLength;
    }
    else
    {
        maxVarCharLength = secondLength;
    }
    if(inclusive)
    {
    // first <= second
        if(memcmp(first, second, maxVarCharLength) <= 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
    // first < second
        if(memcmp(first, second, maxVarCharLength) < 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    

}



/**
 * Function: find a postion for insert a keyPair
 * assign: tempSlot  - the slot that is next to target, 
 *         slotCount - number of slot have iterated
 */
leafSlot* IXFileHandle::inLeafNodeInsertSearch(bool inclusive, void* buffer, LeafNodeHeader* header, Attribute attribute, 
                                  short &slotCount, void* insertKeyValue, int insertKeyLength)
{
    void* comparedKeyValue = NULL;
    int comparedKeyLength = 0;
    slotCount = 0;
    leafSlot* tempSlot = (leafSlot*) ((char*)buffer + PAGE_SIZE);
    // cout<<"slot count: "<<slotCount<<endl;
    while(slotCount < header->slotNum)
    {

        (leafSlot*)tempSlot--; 
        // cout << attribute.type << endl;


        // get the key of record
        if(attribute.type==TypeVarChar)
        {
            // copy the first 4 bytes of a indexPairData which is the length of key to the comparedKeyLength
            memcpy(&comparedKeyLength, (char*)buffer + (tempSlot->offset), sizeof(int));
            comparedKeyValue = malloc(comparedKeyLength);
            // copy the varchar key of a indexPairData to the comparedKey
            memcpy(comparedKeyValue, (char*)buffer + (tempSlot->offset) + sizeof(int), comparedKeyLength);

            // if insertKey > comparedKey
            if(compareVarChar(inclusive, insertKeyValue, insertKeyLength, comparedKeyValue, comparedKeyLength))
            {
                break;
            }
        }
        else if(attribute.type == TypeInt)
        {
            comparedKeyValue = malloc(sizeof(int));
            // copy the first 4 bytes of a indexPairData which is the int/real keyValue
            memcpy(comparedKeyValue, (char*)buffer + (tempSlot->offset), sizeof(int));
            // if insertKey <= comparedKey

            if(inclusive)
            {
                if( *(int*)insertKeyValue <= *(int*)comparedKeyValue )
                {
                    //cout << "insertKey " << *(int*)insertKeyValue << " comparedKey " <<  *(int*)comparedKeyValue << endl;

                    break;
                }
            }
            // if insertKey < comparedKey
            else
            {
                if( *(int*)insertKeyValue < *(int*)comparedKeyValue )
                {
                    // cout << "insertKey " << *(int*)insertKeyValue << " comparedKey " <<  *(int*)comparedKeyValue << endl;

                    break;
                }
            }

            
        }
        else
        {
            // copy the first 4 bytes of a indexPairData which is the int/real keyValue
            comparedKeyValue = malloc(sizeof(int));

            memcpy(comparedKeyValue, (char*)buffer + (tempSlot->offset), sizeof(int));

            //cout<<" comparedKey " <<  *(float*)comparedKeyValue << endl;
            // if insertKey <= comparedKey
            if(inclusive)
            {
                if( *(float*)insertKeyValue <= *(float*)comparedKeyValue )
                {
                    cout << "insertKey " << *(float*)insertKeyValue << " comparedKey " <<  *(float*)comparedKeyValue << endl;
                    break;
                }
            }
            // if insertKey < comparedKey
            else
            {
                if( *(float*)insertKeyValue < *(float*)comparedKeyValue )
                {
                    break;
                }
            }
            
        }
        slotCount++;
    }
    free(comparedKeyValue);
    return tempSlot;
}


RC IXFileHandle::splitLeafNode(void* buffer, void* pairData, 
                               int pairDataLength, LeafNodeHeader* header, 
                               void* insertKeyValue, int insertKeyLength, int slotCount, int oldPageNum, Attribute attribute, const void *key, const RID &rid)
{
    
    // 1. find two middle nodes
    void* keyMiddleS; // smaller middle key
    unsigned keyMiddleSLength;
    leafSlot* keyMiddleSPosition; // keyMiddleSmaller split position
    void* keyMiddleB; // bigger middle key
    unsigned keyMiddleBLength;
    leafSlot* keyMiddleBPosition; // keyMiddleBigger split position

    void* realKeyMiddle; // real split middle key
    int realKeyMiddleLength = 0;
    leafSlot* realMiddlePositon; // real split postion

    leafSlot* tempSlot = (leafSlot*) ((char*)buffer + PAGE_SIZE);
    // cout << "first index ->startOffset" << (leafSlot*)(tempSlot-1)->offset << endl;

    int splitPostion = -1;
    

    if(header->slotNum > 1)
    {
        // if slotNum is even, find the middle two
        if(header->slotNum%2 == 0)
        {
            tempSlot -= (header->slotNum/2);
            splitPostion = header->slotNum/2;
            // cout << "header->slotNum" << header->slotNum;
            // cout << "splitPosition" << splitPostion << endl;
            // cout << "split offset" << tempSlot->offset << endl;

            
        }
        else
        {
            tempSlot -= (header->slotNum/2 + 1);
            splitPostion = header->slotNum/2 + 1;
            // cout << "header->slotNum" << header->slotNum;
            // cout << "splitPosition" << splitPostion << endl;
            

        }

        

        if(attribute.type == TypeVarChar)
        {
            // copy middle key
            memcpy(&keyMiddleSLength, (char*)buffer + (tempSlot->offset), sizeof(unsigned));
            keyMiddleS = malloc(keyMiddleSLength);
            memcpy(keyMiddleS, (char*)buffer + (tempSlot->offset) + sizeof(unsigned), keyMiddleSLength);
            keyMiddleSPosition = tempSlot;

            (leafSlot*)tempSlot--;
            // copy middleB key
            memcpy(&keyMiddleBLength, (char*)buffer + (tempSlot->offset), sizeof(unsigned));
            keyMiddleB = malloc(keyMiddleBLength);
            memcpy(keyMiddleB, (char*)buffer + (tempSlot->offset) + sizeof(unsigned), keyMiddleBLength);
            keyMiddleBPosition = tempSlot;

            if(compareVarChar(true, insertKeyValue, insertKeyLength, keyMiddleS, keyMiddleSLength))
            {
                realKeyMiddle = keyMiddleS;
                realKeyMiddleLength = keyMiddleSLength;
                realMiddlePositon = keyMiddleSPosition;
            }
            else if(compareVarChar(true, insertKeyValue, insertKeyLength, keyMiddleB, keyMiddleBLength))
            {
                realKeyMiddle = insertKeyValue;
                realKeyMiddleLength = insertKeyLength;

                realMiddlePositon = keyMiddleSPosition; // the split position for insert key is the same as Smaller Middle Key
            }
            else
            {
                realKeyMiddle = keyMiddleB;
                realKeyMiddleLength = keyMiddleBLength;

                realMiddlePositon = keyMiddleBPosition;
            }
        }
        if(attribute.type == TypeInt)
        {
            // copy middle key
            keyMiddleS = malloc(sizeof(int));
            memcpy(keyMiddleS, (char*)buffer + (tempSlot->offset), sizeof(int));
            keyMiddleSPosition = tempSlot;

            (leafSlot*)tempSlot--;
            // copy middleB key
            keyMiddleB = malloc(sizeof(int));
            memcpy(keyMiddleB, (char*)buffer + (tempSlot->offset), sizeof(int));
            keyMiddleBPosition = tempSlot;
            if(*(int*)insertKeyValue <= *(int*)keyMiddleS)
            {
                realKeyMiddle = keyMiddleS;
                realMiddlePositon = keyMiddleSPosition;
            }
            else if(*(int*)insertKeyValue <= *(int*)keyMiddleB)
            {
                splitPostion = slotCount;
                realKeyMiddle = insertKeyValue;
                realMiddlePositon = keyMiddleSPosition; // the split position for insert key is the same as Smaller Middle Key
            
            }
            else
            {
                realKeyMiddle = keyMiddleB;
                realMiddlePositon = keyMiddleBPosition;
            }
        }
        if(attribute.type == TypeReal)
        {
            // copy middle key
            keyMiddleS = malloc(sizeof(int));
            memcpy(keyMiddleS, (char*)buffer + (tempSlot->offset), sizeof(int));
            keyMiddleSPosition = tempSlot;

            (leafSlot*)tempSlot--;
            // copy middleB key
            keyMiddleB = malloc(sizeof(int));
            memcpy(keyMiddleB, (char*)buffer + (tempSlot->offset), sizeof(int));
            keyMiddleBPosition = tempSlot;
            if(*(float*)insertKeyValue <= *(float*)keyMiddleS)
            {
                realKeyMiddle = keyMiddleS;
                realMiddlePositon = keyMiddleSPosition;
            }
            else if(*(float*)insertKeyValue <= *(float*)keyMiddleB)
            {
                splitPostion = slotCount;
                realKeyMiddle = insertKeyValue;
                realMiddlePositon = keyMiddleSPosition; // the split position for insert key is the same as Smaller Middle Key
            
            }
            else
            {
                realKeyMiddle = keyMiddleB;
                realMiddlePositon = keyMiddleBPosition;
            }
        }
        
    }

    // 3. construct the new page
    int newPageId = -1;
    void* newPageBuffer = malloc(PAGE_SIZE);
    int newPageSlotNum = -1;

    newPageSlotNum = header->slotNum - splitPostion;

    newPageId = appendLeafNode();
    if(indexFileHandle.readPage(newPageId, newPageBuffer))
    {
        cout << "readPage fail" << endl;
        return -1;
    }

    LeafNodeHeader* newPageHeader = (LeafNodeHeader*) newPageBuffer;

    // 4. copy the indexPairs into new leaf page
    // src: buffer [realMiddlePostion->offset, header->startOffset]
    // dst: newPageBuffer [header->startOffset]
    // int value;
    // memcpy(&value, keyMiddleB, 4);
    // cout << "value" <<value<<  endl;
    // // cout << realKeyMiddle << " " << keyMiddleS << " " << keyMiddleB << endl;
    // memcpy(&value, insertKeyValue, 4);
    // cout << "value: " << value<< endl;
    // memcpy(&value, keyMiddleS, 4);
    // cout << "value: " << value<< endl;
    // memcpy(&value, realKeyMiddle, 4);
    // cout << "value: " << value<< endl;
    // cout << "newPageSlotNum " << newPageSlotNum <<endl;
    // cout << "splitPostion " << splitPostion << endl;

    // cout << "newPageHeader->startOffset" << keyMiddleSPosition->offset << endl;
    // cout << "realMiddlePositon->offset   " << realMiddlePositon->offset << endl;
    // cout << "keyMiddleBPostion->offset " << keyMiddleBPosition->offset << endl;
    // cout << "header->startOffset: " << header->startOffset << endl;
    memcpy((char*)newPageBuffer + newPageHeader->startOffset, (char*) buffer + realMiddlePositon->offset, 
            header->startOffset-realMiddlePositon->offset);
    // cout << "copyed index number: " << (header->startOffset-realMiddlePositon->offset)/12 << endl;



    // 5. copy the splited slots into new Page
    // src: buffer [PAGE_SIZE- header->slotNum*sizeof(leafSlot), (leafSlot*)readMiddlePostion+1]
    // dst: newPageBuffer [PAGE_SIZE - (header->slotNum-slotCount+1)*sizeof(leafSlot), PAGE_SIZE]
    memcpy((char*)newPageBuffer + PAGE_SIZE - newPageSlotNum*sizeof(leafSlot),
           (char*)buffer+ PAGE_SIZE - header->slotNum*sizeof(leafSlot), 
           newPageSlotNum*sizeof(leafSlot));
    // cout << "copyed slot number: " << newPageSlotNum << endl;
    
    // 6. clear the old position for splited page to 0
    
    // memset((char*) buffer + realMiddlePositon->offset, 0, 
    // PAGE_SIZE - (slotCount-1)*sizeof(leafSlot) - realMiddlePositon->offset); // untested ???
    
    // cout << "here" << endl;


    // 7. update new page header and header
    newPageHeader->startOffset = header->startOffset-realMiddlePositon->offset+sizeof(LeafNodeHeader);
    newPageHeader->slotNum = newPageSlotNum;
    newPageHeader->freespace = PAGE_SIZE-(newPageHeader->slotNum * sizeof(leafSlot) + newPageHeader->startOffset);
    newPageHeader->parentNodeId = header->parentNodeId; // not sure ???
    newPageHeader->nextLeafId = header->nextLeafId;
    header->nextLeafId = newPageId;
    header->startOffset = realMiddlePositon->offset; // set the old page startOffset to the offset of the first deleted index
    header->slotNum -= newPageSlotNum;
    header->freespace += newPageHeader->slotNum * sizeof(leafSlot) + newPageHeader->startOffset;

    // 8. update slot->offset s
    leafSlot* tmp = (leafSlot*) ((char*)newPageBuffer + PAGE_SIZE);
    int count = 0;
    while(count < newPageSlotNum)
    {
        (leafSlot*)tmp--;
        tmp->offset -= (realMiddlePositon->offset-sizeof(LeafNodeHeader));
        count++;
    }

    
    indexFileHandle.writePage(oldPageNum, buffer);
    indexFileHandle.writePage(newPageId, newPageBuffer);

    //cout<<"start copy up"<<endl;

    // 9. copy up ***
    // input (oldPageId, newPageId, readKeyMiddle)
    // return (oldPageParent, newPageParent)
    void* varCharRealKey = malloc(realKeyMiddleLength+sizeof(int));

    int parent = -1;
    if(header->parentNodeId == -1)
    {
        parent = appendNonLeafNode();
        // cout<<"page number is:" <<indexFileHandle.getNumberOfPages()<<endl;
        // cout<<"parent numer is: "<<parent<<endl;
        if(attribute.type == TypeVarChar)
        {
            memcpy(varCharRealKey, &realKeyMiddleLength, sizeof(int));
            memcpy((char*)varCharRealKey+sizeof(int), realKeyMiddle, realKeyMiddleLength);
            if(insertIndexToNonLeaf(parent, oldPageNum, newPageId, attribute, varCharRealKey))
                return -1;
        }
        else
        {
            if(insertIndexToNonLeaf(parent, oldPageNum, newPageId, attribute, realKeyMiddle))
                return -1;
        }

        setRootPageNum(parent);
        header->parentNodeId = parent;
        newPageHeader->parentNodeId = header->parentNodeId;
        indexFileHandle.writePage(oldPageNum, buffer);
        indexFileHandle.writePage(newPageId, newPageBuffer);
    }
    else
    {
        if(attribute.type == TypeVarChar)
        {
            memcpy(varCharRealKey, &realKeyMiddleLength, sizeof(int));
            memcpy((char*)varCharRealKey+sizeof(int), realKeyMiddle, realKeyMiddleLength);
            if(insertIndexToNonLeaf(header->parentNodeId, oldPageNum, newPageId, attribute, varCharRealKey))
                return -1;

        }
        else
        {
            if(insertIndexToNonLeaf(header->parentNodeId, oldPageNum, newPageId, attribute, realKeyMiddle))
                return -1;
        }    
    }
    // cout<<"end copy up"<<endl;
 


    // cout << "insert New Index" << endl;
    insertIndexToLeaf(newPageId, attribute, key, rid);
    // cout << "end insert new index" << endl;
    // copy up ***
    free(buffer);
    free(keyMiddleB);
    free(keyMiddleS);
    free(newPageBuffer);
    return 0;
}
/**
 * Function: find a index pair
 */
leafSlot* IXFileHandle::inLeafNodeIndexSearch(void* buffer, leafSlot* tempSlot, 
                                       LeafNodeHeader* header, 
                                       short &slotCount, const RID &rid)
{
    slotCount = 0;
    tempSlot = (leafSlot*) ((char*)buffer + PAGE_SIZE);
    RID* r;
    while(slotCount < header->slotNum)
    {
        (leafSlot*)tempSlot--; 
        r = (RID*)((char*)buffer + tempSlot->offset + tempSlot->rLeng - sizeof(RID));
        if(r->slotNum == rid.slotNum && r->pageNum == rid.pageNum)
        {
            return tempSlot;
        }
        slotCount++;
        // get the rid of record
    }
    if(slotCount == header->slotNum)
    {
        cout << "cannnot find the node" << endl;
        return NULL;
    }
    return tempSlot;

}


/**
 * Funtion: delete a pair of (key, rid) given a page
 */
RC IXFileHandle::deleteIndexOfLeaf(int pageNum, const Attribute &attribute, const void *key, const RID &rid)
{
    // 1. allocate a buffer to load the given page
    void* buffer = malloc(PAGE_SIZE);
    if(indexFileHandle.readPage(pageNum, buffer))
    {
        cout << "readPage fail" << endl;
        return -1;
    }
    LeafNodeHeader* header = (LeafNodeHeader*)buffer;

    short slotCount = 0;
    leafSlot* tempSlot; // slot which is the first index bigger than insertKey

    // 2. search the postion for the key with rid
    tempSlot = inLeafNodeIndexSearch(buffer, tempSlot, header, slotCount, rid);
    if(tempSlot == NULL)
    {
        cout << "cannot delete a index which doesn't exist." << endl;
        return -1;
    }
    // 3. delete the record pair
    int deleteRLeng = tempSlot->rLeng;

    if(header->slotNum > 1)
    {
        // cout << "slotNUmber Header: " << header->slotNum << endl;
        // cout << "slotCount" << slotCount << endl;
        leafSlot* nextSlot = (leafSlot*)tempSlot-1;
        // cout << "tempSlot:" << ((char*)buffer+PAGE_SIZE-(char*)tempSlot)/4 << endl;
        // cout << "tempSlot offset" << tempSlot->offset/12 - 1 << endl;
        memset((char*)buffer+tempSlot->offset, 0, tempSlot->rLeng); // clear current index

        int sCount = slotCount;
        if(sCount != header->slotNum -1)
        {
            memmove((char*)buffer+tempSlot->offset, (char*)buffer+(nextSlot->offset), // adjust next indexes
                header->startOffset - nextSlot->offset);
            // cout << "length of moved: " << header->startOffset - nextSlot->offset << endl;

        // 4. update slots' offsets
            leafSlot* tmp = tempSlot;
            // cout << "header slotNum:"
            while(sCount < header->slotNum - 1)
            {
                (leafSlot*)tmp--;
        // cout << "tmp:" << ((char*)buffer+PAGE_SIZE-(char*)tmp)/4 << endl;

                tmp->offset -= tempSlot->rLeng;
                sCount++;
                // cout << "sCount"<<sCount << endl;
            }

        // 5. delete the slot
            // cout << "header->slotNum-1 " << header->slotNum-1<< endl;
            // cout << "(header->slotNum - slotCount+1 " << (header->slotNum - slotCount+1) << endl;

            memmove((char*)buffer + PAGE_SIZE-(header->slotNum-1) * sizeof(leafSlot), 
                (char*)buffer + PAGE_SIZE- header->slotNum * sizeof(leafSlot),
                (header->slotNum - slotCount-1)*sizeof(leafSlot));
        }
    }
    

    // 6. clear the last slot
    memset((char*)buffer + PAGE_SIZE - (header->slotNum) * sizeof(leafSlot), 0, sizeof(leafSlot));

    // 7. update the header
    header->slotNum--;
    header->freespace += (deleteRLeng + sizeof(leafSlot));
    header->startOffset -= deleteRLeng;

    // 8. write into page
    indexFileHandle.writePage(pageNum, buffer);

    free(buffer);

    return 0;
}

/**
 * Function: write a pair of (key, rid) into a give leafnode page.
 */
RC IXFileHandle::insertIndexToLeaf(int pageNum, const Attribute &attribute, const void *key, const RID &rid)
{

    // cout << "RID" << rid.slotNum << rid.pageNum << endl;
    // 1. prepare a void* pairData of a data pair
    void* pairData;
    void *buffer = malloc(PAGE_SIZE);
    int pairDataLength = 0;
    short slotCount = 0;

    unsigned insertKeyLength = 0;

    //  cout << "insert page: " << pageNum << endl;

    // cout << "pageNum: " << indexFileHandle.getNumberOfPages() << endl;


    if(indexFileHandle.readPage(pageNum, buffer) == -1)
    {
        cout << "readPage fail" << endl;
        return -1;
    }

    pairData = pairAnalysis(attribute, key, rid, pairDataLength);
    // cout << pairDataLength  << endl;

    // 2. get header of leaf node
    LeafNodeHeader * header = (LeafNodeHeader*)buffer;

    // 3. check if it is inserting into a leaf node
    if(header->isLeaf == false)
    {
        cout << "cannot insert into a nonleaf node" << endl;
        return -1;
    }

    void* insertKeyValue;

    // 4. find the corresponding postion for the record and
    if(attribute.type == TypeVarChar)
    {
        memcpy(&insertKeyLength, (char*)key, sizeof(unsigned));
        insertKeyValue = malloc(insertKeyLength);

        memcpy(insertKeyValue, (char*)key+sizeof(unsigned) , insertKeyLength);
    }
    else
    {
        insertKeyLength = sizeof(unsigned);
        insertKeyValue = malloc(insertKeyLength);
        memcpy(insertKeyValue, (char*)key, sizeof(unsigned));

    }

    // assign: tempSlot  - the slot that is next to target, 
    //         slotCount - number of slot have iterated
    leafSlot* tempSlot = inLeafNodeInsertSearch(false, buffer, header, attribute, 
                     slotCount, insertKeyValue, insertKeyLength);

    // cout << "slot count: " << slotCount << endl;

    // 5. check if there is enough space, if not split leaf node.
    // cout << "freespace: " << header->freespace << endl;
    // cout << "need space: " << pairDataLength+sizeof(leafSlot)
    // cout<<"freespace: "<<header->freespace<<endl;
    if(header->freespace <= pairDataLength+sizeof(leafSlot)){
        // cout << "no enough space in page: "<<pageNum << endl;
        // split leaf node
        if(splitLeafNode(buffer, pairData, pairDataLength, header, insertKeyValue, insertKeyLength, slotCount, pageNum, attribute, key, rid))
        {
            // cout << "splitLeadNode fail" << endl;
            return -1;
        }
        return 0;
    }


    // 6. insert new slot and new indexPair
    // if need to insert in the middle:
    //      new slot: position = tempSlot, offset = tempSlot->offset, 
    //      
    //      new pair: poistion = tempSlot->offset

    // if need to insert in the middle
    // cout << "slotcount" << slotCount <<endl;
    // cout << "header slot Num: " << header->slotNum << endl;


    if(slotCount != header->slotNum)
    {
        // 6.1 move and insert slot
        // Move: move slots nexts to it a slotsize step.
        memmove((char*)buffer + PAGE_SIZE - (header->slotNum+1)*sizeof(leafSlot), 
               (char*)buffer + PAGE_SIZE - (header->slotNum)*sizeof(leafSlot), 
               sizeof(leafSlot)*(header->slotNum - slotCount));

        // Insert: tempSlot point to the new slot, its offset don't change, length change.
        tempSlot->rLeng = pairDataLength;
        // cout << "header slot Num: " << header->slotNum << endl;
        header->slotNum++;
        // Update next slots' offset
        leafSlot* tmp = tempSlot;
        // cout << slotCount << " " << header->slotNum << endl;
        
        
        while(slotCount < header->slotNum)
        {
        // cout << "slotcount" << slotCount <<endl;
        // cout << "header slot Num: " << header->slotNum << endl;

            (leafSlot*)tmp--; 

            tmp->offset += pairDataLength;
            slotCount++;
        }
            // cout << "here2" << endl;

        // 6.2 move and insert indexPairData
        // Move: the indexes forwards: tempSlot now is the index that should be put next to the insert index
        // - stepLength: pairDataLength, - fromPostion: tempSlot->offset, - range: [tempSlot, header->startOffset]
        // cout << header->startOffset << endl;
        // cout << tempSlot->offset << endl;

        // cout << tempSlot->offset + tempSlot->rLeng << " " << tempSlot->offset << " " << header->startOffset - tempSlot->offset << endl;
        memmove((char*)buffer + tempSlot->offset + tempSlot->rLeng, (char*)buffer + tempSlot->offset, 
                header->startOffset - tempSlot->offset);


        // cout << "tempSlot->offset " << tempSlot->offset << endl;
        
        memcpy((char*)buffer+tempSlot->offset, pairData, pairDataLength);

        // 6.3 Updata header->startOffset and header->freeSpace
        // cout << "pairDataLength" << pairDataLength << endl;
        header->startOffset += pairDataLength;
        header->freespace -= (pairDataLength + sizeof(leafSlot));


    }

    // if can just insert behind
    else
    {
        // cout<<"insert at the tail"<<endl;
        // 6.1 insert slot
        (leafSlot*)tempSlot--;
        tempSlot->offset = header->startOffset;
        // cout << "tempSlot offset: " << tempSlot->offset << endl;
        tempSlot->rLeng = pairDataLength;
        header->slotNum++;
        // cout<<"slot updated"<<endl;

        // 6.2 insert pairData
        memcpy((char*)buffer+header->startOffset,pairData,pairDataLength);

        // 6.3 Updata header->startOffset and header->freeSpace
        // cout << "pairDatalength: " << pairDataLength << endl;
        header->startOffset += pairDataLength;
        header->freespace -= (pairDataLength + sizeof(leafSlot));
    }
    // 7. write buffer into page
    // cout << "here begin insert" << endl;

    indexFileHandle.writePage(pageNum, buffer);

    // 8. clear pairData and buffer
    free(pairData);
    free(buffer);
    free(insertKeyValue);

    return 0;
}

/**
 * Function: get the type of leaf/nonleaf of a page
 * Return: true/false
 */
bool IXFileHandle::getPageType(int pageNum)
{
    // 1. get the content of the given page
    bool isLeaf;
    void* buffer = malloc(PAGE_SIZE);
    if(indexFileHandle.readPage(pageNum, buffer))
    {
        cout << "readPage fail, Pagenum:" << pageNum << endl;
        return -1;
    }

    // 2. get the first byte which is a bool indicate True/False
    memcpy(&isLeaf, buffer, 1);
    return isLeaf;
}

void moveNonleafRecords(void* buffer, int startOffset, int endOffset, int moveLength, int direction){
    //for direction: 1->right 0->left
    NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
    nonleafSlot* tempslot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-header->slotNum*sizeof(nonleafSlot));

    for(unsigned i = 0; i<header->slotNum; i++){
        int tempOffset = tempslot->offset;
        //move the key
        memmove((byte*)buffer+tempslot->offset+moveLength,(byte*)buffer+tempslot->offset, tempslot->klength+4);
        tempslot->offset += moveLength;
        //move the slots
        memmove(tempslot-1, tempslot, sizeof(nonleafSlot));

        if(tempOffset == startOffset)
            break;
        tempslot++;
    }
}


RC IXFileHandle::insertIndexToNonLeaf(int pageNum, int sPageId, int lPageId, const Attribute &attribute, const void *key){
     void* buffer = malloc(PAGE_SIZE);
    //  cout<<"page number is: "<<pageNum<<endl;
     if(indexFileHandle.readPage(pageNum,buffer) == -1){
         free(buffer);
         return -1;
     }

    //  cout<<"value is "<<*(int*)key<<endl;

    NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
    //determine if freespace is enough
    int varLength;
    if(attribute.type == TypeVarChar){
        varLength = *(int*)key+sizeof(int);
    }
    else
        varLength = 4+sizeof(int);
    if(varLength > header->freespace){
        //need to split non-leafpage
        if(splitNonLeafNode(pageNum,sPageId,lPageId,attribute,key,buffer) == -1){
            free(buffer);
            return -1;
        }
        free(buffer);
        return 0;
    }

    //no need to split, find the right position and insert
    bool flag = false; //to indicate if insert has happened
    nonleafSlot* tempslot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-sizeof(nonleafSlot));
    // cout<<"slotNum: "<<header->slotNum<<endl;
    for(unsigned i = 0; i<header->slotNum; i++){
        //find the corresponding value and do the compare
        byte* keyValue = (byte*)buffer + tempslot->offset;

        //move all the value bigger than key to the right
        //then copy the key value to the position
        //then update the left and right pointer
        int cpOffset = tempslot->offset;
        if(attribute.type == TypeInt){
            //interpret both values using int
            if(*(int*)keyValue >= *(int*)key){

                moveNonleafRecords(buffer, tempslot->offset, header->startOffset, 4+4, 1);
                memcpy((byte*)buffer+cpOffset,key,4);

                //update the pointers
                *(int*)((byte*)buffer+cpOffset-4) = sPageId;
                *(int*)((byte*)buffer+cpOffset+4) = lPageId;

                //update the slot
                tempslot->klength = varLength;
                tempslot->offset = cpOffset;

                flag = true;
                header->slotNum++;
                break;
            }
        }
        else if(attribute.type == TypeReal){
            if(*(float*)keyValue >= *(float*)key){
                
                moveNonleafRecords(buffer, tempslot->offset, header->startOffset, 4+4, 1);
                memcpy((byte*)buffer+cpOffset,key,4);

                //update pointers
                *(int*)((byte*)buffer+cpOffset-4) = sPageId;
                *(int*)((byte*)buffer+cpOffset+4) = lPageId;

                //update the slot
                tempslot->klength = varLength;
                tempslot->offset = cpOffset;

                flag = true;
                header->slotNum++;
                break;
            }
        }
        else if(attribute.type == TypeVarChar){
            //get the length of string
            int* varLength = (int*)key;
            //move pointer to value
            void* keyString = (byte*)key+4;
            int cmpLength = ((*varLength)>tempslot->klength)?(*varLength):tempslot->klength;
            if(memcmp(keyValue,keyString,cmpLength) >= 0){

                moveNonleafRecords(buffer, tempslot->offset, header->startOffset, *varLength+4, 1);
                //copy the key
                memcpy((byte*)buffer+cpOffset,key,*varLength);

                //update the pointers
                *(int*)((byte*)buffer+cpOffset-4) = sPageId;
                *(int*)((byte*)buffer+cpOffset+*varLength) = lPageId;

                //update the slot
                tempslot->klength = *varLength;
                tempslot->offset = cpOffset;

                flag = true;
                header->slotNum++;
                break;
            }
        }
        //move slot
        tempslot--;
    }

    if(flag == false){
        //if didn't find a slot to insert, then increase slots
        int startOffset;
        if(header->slotNum == 0)
            startOffset = header->startOffset;
        else
            //we need to modify left pointer of last key
            startOffset = header->startOffset - sizeof(int);
        //write the leftpointer
        memcpy((byte*)buffer+startOffset, &sPageId, 4);
        startOffset += 4;
        //update freespace
        header->freespace -= 4;

        //copy value
        tempslot->offset = startOffset;
        if(attribute.type == TypeVarChar){
            int* varLength = (int*)key;
            //move pointer to value
            void* keyString = (byte*)key+4;
            memcpy((byte*)buffer+startOffset,keyString, *varLength);
            tempslot->klength = *varLength;
            startOffset += *varLength;
            //update freespace
            header->freespace -= *varLength;
        }
        else{
            memcpy((byte*)buffer+startOffset,key, 4);
            tempslot->klength = 4;
            startOffset += 4;
            //update freespace
            header->freespace -= 4;
        }
        //set the right pointer
        memcpy((byte*)buffer+startOffset, &lPageId, 4);
        startOffset += 4;

        //update the new startoffset
        header->startOffset = startOffset;
        //update the new slot number
        header->slotNum++;
        //update freespace
        header->freespace -= 4;
    }
    // cout<<header->slotNum<<endl;
    indexFileHandle.writePage(pageNum, buffer);
    free(buffer);
    // cout<<"insert successfully"<<endl;
    return 0;
 }

 void IXFileHandle::updateNonleafNodeChilds(int pageNum){
     void* newBuffer = malloc(PAGE_SIZE);
     indexFileHandle.readPage(pageNum,newBuffer);
     NonLeafNodeHeader* nHeader = (NonLeafNodeHeader*)newBuffer;

    //go through all the child nodes of the new generated non leaf node to modify the parent node
    nonleafSlot* cpSlot = (nonleafSlot*)((byte*)newBuffer+PAGE_SIZE-sizeof(nonleafSlot));
    for(int i = 0; i<nHeader->slotNum; i++){
        void* tempBuffer = malloc(PAGE_SIZE);
        // cout<<"offset: "<<cpSlot->offset<<endl;
        int tempNum = *(int*)((byte*)newBuffer+cpSlot->offset-4);
        // cout<<"temp number is:"<<tempNum<<endl;
        indexFileHandle.readPage(tempNum, tempBuffer);
        bool* ifLeaf = (bool*)tempBuffer;
        if(ifLeaf){
            LeafNodeHeader* tempHeader = (LeafNodeHeader*)tempBuffer;
            tempHeader->parentNodeId = pageNum;
        }
        else{
            NonLeafNodeHeader* tempHeader = (NonLeafNodeHeader*)tempBuffer;
            tempHeader->ParentNodeId = pageNum;
        }
        indexFileHandle.writePage(tempNum,tempBuffer);
        free(tempBuffer);
        if(i == nHeader->slotNum-1){
            tempBuffer = malloc(PAGE_SIZE);
            tempNum = *(int*)((byte*)newBuffer+cpSlot->offset+cpSlot->klength);
            // cout<<"temp number is:"<<tempNum<<endl;
            indexFileHandle.readPage(tempNum, tempBuffer);
            byte* ifLeaf = (byte*)tempBuffer;
            if(ifLeaf){
                LeafNodeHeader* tempHeader = (LeafNodeHeader*)tempBuffer;
                // cout<<tempHeader->parentNodeId<<endl;
                // cout<<tempHeader->nextLeafId<<endl;
                tempHeader->parentNodeId = pageNum;
                // cout<<"parent modified"<<endl;
            }
            else{
                NonLeafNodeHeader* tempHeader = (NonLeafNodeHeader*)tempBuffer;
                tempHeader->ParentNodeId = pageNum;
            }
            indexFileHandle.writePage(tempNum,tempBuffer);
            free(tempBuffer);
            break;
        }
        cpSlot--;
    }
    free(newBuffer);

 }

RC IXFileHandle::splitNonLeafNode(int pageNum, int sPageId, int lPageId, const Attribute &attribute, const void *key, void* buffer){
    //find from where we should divide the keys
    //append a new nonleaf node to copy the larger keys
    //push the middle value to the parent non-leaf node
    static int t = 0;
    t++;
    // cout<<"split non leaf node: "<< t <<endl;

    NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
    int newNonleafPage = appendNonLeafNode();


    int comNum = ceil((double)header->slotNum/2);
    //get the corresponding slot
    nonleafSlot* tempslot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-comNum*sizeof(nonleafSlot));

    void* comValue = (byte*)buffer + tempslot->offset;
    int splitNum; //indicate from which key should be moved to the new page
    //used to indicate new record should be inserted into which page, true means need to be inserted into new page
    bool insertFlag = false; 

    if(attribute.type == TypeVarChar){
        //get the length of string
        int* varLength = (int*)key;
        //move pointer to value
        void* keyString = (byte*)key+4;
        int cmpLength = ((*varLength)>tempslot->klength)?(*varLength):tempslot->klength;
        if(memcmp(comValue,keyString,cmpLength) >= 0)
            splitNum = comNum;
        else{
            splitNum = comNum + 1;
            insertFlag = true;
        }
    }
    else if(attribute.type == TypeInt){
        if(*(int*)comValue >= *(int*)key){
            splitNum = comNum;
        }
        else{
            splitNum = comNum + 1;
            insertFlag = true;
        }

    }
    else if(attribute.type == TypeReal){
        if(*(float*)comValue >= *(float*)key)
            splitNum = comNum;
        else{
            splitNum = comNum + 1;
            insertFlag = true;
        }
    }

    //move key to new page 
    void* newBuffer = malloc(PAGE_SIZE);
    if(indexFileHandle.readPage(newNonleafPage, newBuffer) == -1)
        return -1;
    NonLeafNodeHeader* nHeader = (NonLeafNodeHeader*) newBuffer;

    nonleafSlot* copySlot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-(splitNum+1)*sizeof(nonleafSlot));

    // cout<<"header->slotnum: "<<header->slotNum<<endl;

    //get the copy offset
    int cpyStartOffset = copySlot->offset-4;//include one left pointer
    int cpyEndOffset = header->startOffset;
    int cpySlotStartOffset = PAGE_SIZE-header->slotNum*sizeof(nonleafSlot);
    int cpySlotEndOffset = PAGE_SIZE-splitNum*sizeof(nonleafSlot);
    int moveNum = header->slotNum-splitNum;

    // cout<<"the slot move to the new node: "<<moveNum<<endl;

    for(int i = splitNum+1; i<=header->slotNum; i++){
        //update the slots
        copySlot->offset -= cpyStartOffset-sizeof(NonLeafNodeHeader);
        // cout<<"offset: "<<copySlot->offset<<endl;
        copySlot--;
    }

    //copy the keys to the new page
    memcpy((byte*)newBuffer+nHeader->startOffset,(byte*)buffer+cpyStartOffset, cpyEndOffset-cpyStartOffset);
    header->startOffset = cpyStartOffset;
    header->freespace += cpyEndOffset-cpyStartOffset;
    nHeader->startOffset += cpyEndOffset-cpyStartOffset;
    nHeader->freespace -= cpyEndOffset-cpyStartOffset;

    //copy the slots to the new page
    memcpy((byte*)newBuffer+PAGE_SIZE-moveNum*sizeof(nonleafSlot), (byte*)buffer+cpySlotStartOffset, cpySlotEndOffset-cpySlotStartOffset);
    header->slotNum -= moveNum;
    header->freespace += moveNum*sizeof(nonleafSlot);
    nHeader->slotNum += moveNum;
    nHeader->freespace -= moveNum*sizeof(nonleafSlot);
    
    //insert the middle key into the parent nonleaf page
    nonleafSlot* middleSlot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-splitNum*sizeof(nonleafSlot));
    short parentPageNum = header->ParentNodeId;
    if(parentPageNum == -1){
        parentPageNum = appendNonLeafNode();
        header->ParentNodeId = parentPageNum;
        //this should be the new root node
        setRootPageNum(parentPageNum);
    }
    void* insertKey;
    if(attribute.type == TypeVarChar){
        insertKey = malloc(middleSlot->klength+sizeof(int));
        *(int*)insertKey = middleSlot->klength;
        memcpy((byte*)insertKey+4,(byte*)buffer+middleSlot->offset,middleSlot->klength);
    }
    else{
        insertKey = malloc(middleSlot->klength);
        memcpy((byte*)insertKey,(byte*)buffer+middleSlot->offset,middleSlot->klength);
    }

    // cout<<"insert key is: "<<*(int*)insertKey<<endl;
    insertIndexToNonLeaf(parentPageNum,pageNum,newNonleafPage,attribute,insertKey);
    //delete this key from original node
    header->startOffset = middleSlot->offset;
    header->freespace += middleSlot->klength + 4;
    header->slotNum--;
    //mark the new nonleaf's parent page
    nHeader->ParentNodeId = parentPageNum;


    indexFileHandle.writePage(pageNum, buffer);
    indexFileHandle.writePage(newNonleafPage,newBuffer);

    //determine whether we need to insert the key into original page or new page
    if(insertFlag)
        insertIndexToNonLeaf(newNonleafPage,sPageId,lPageId,attribute,key);
    else
        insertIndexToNonLeaf(pageNum,sPageId,lPageId,attribute,key);
    
    free(insertKey);
    free(newBuffer);

    updateNonleafNodeChilds(newNonleafPage);

}


/**
 * return: >-1 pageId of node of the key
 * return: -1  no root page
 * return: -2  cannot find the key
 */
RC IXFileHandle::searchNode(const Attribute &attribute, const void *key){

    // 1. get root pageId
    int rootPageNum = getRootPageNum();
    int firstLeafPageId = -1;

    // cout<<"root page: "<<rootPageNum<<endl;
    
    // if this is no root page
    if(rootPageNum == -1)
    {
        return rootPageNum;
    }

    void * buffer = malloc(PAGE_SIZE);
    int curPage = rootPageNum;
    while(1){
        // cout<<"curpage: "<<curPage<<endl;
        if(indexFileHandle.readPage(curPage, buffer) == -1)
            return -2;
        byte* leafcheck = (byte*)buffer;
        //if current page is a leafpage
        if(*leafcheck == true)
            return curPage;
        //it's a non leafpage, so we begin the compare
        NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
        // cout<<"slot number: "<<header->slotNum<<endl;

        nonleafSlot* tempslot = (nonleafSlot*)((byte*)buffer+PAGE_SIZE-sizeof(nonleafSlot));
        for(unsigned i = 0; i<header->slotNum; i++){
            //find the corresponding value and do the compare
            byte* keyValue = (byte*)buffer + tempslot->offset;
            if(attribute.type == TypeInt){
                //interpret both values using int
                if(*(int*)keyValue >= *(int*)key){
                    //return the left pointer since it's smaller value
                    curPage = *(int*)(keyValue - 4);
                    break;
                }
            }
            else if(attribute.type == TypeReal){
                if(*(float*)keyValue >= *(float*)key){
                    //return the left pointer since it's smaller value
                    curPage = *(int*)(keyValue - 4);
                    break;
                }
            }
            else if(attribute.type == TypeVarChar){
                //get the length of string
                int* varLength = (int*)key;
                //move pointer to value
                void* keyString = (byte*)key+4;
                int cmpLength = ((*varLength)>tempslot->klength)?(*varLength):tempslot->klength;
                if(memcmp(keyValue,keyString,cmpLength) >= 0){
                    curPage = *(int*)(keyValue - 4);
                    break;
                }
            }
            if(i == header->slotNum-1){
                curPage = *(int*)(keyValue+tempslot->klength);
                break;
            }
            //move slot
            tempslot--;
        }
    }
    free(buffer);

    return curPage;
}

/**
 * return: the PageId of new page appended.
 */
int IXFileHandle::appendLeafNode(){
    int newPageId = -1;

    // 1. allocate a new buffer to store the page
    void* buffer = malloc(PAGE_SIZE);
    memset(buffer, 0, PAGE_SIZE);

    // 2. configure directory
    LeafNodeHeader * lnh = (LeafNodeHeader*)buffer;
    lnh->isLeaf = true;
    lnh->freespace = PAGE_SIZE-sizeof(LeafNodeHeader);
    lnh->nextLeafId = -1;
    lnh->parentNodeId = -1;
    lnh->startOffset = sizeof(LeafNodeHeader);
    lnh->slotNum = 0;

    // 3. append this buffer to a new page
    indexFileHandle.appendPage(buffer);
    newPageId = indexFileHandle.getNumberOfPages()-1;

    return newPageId;
}

RC IXFileHandle::appendNonLeafNode(){
    //init non leaf page frame
    void* buffer = malloc(PAGE_SIZE);
    memset(buffer,0,PAGE_SIZE);

    NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
    header->isLeaf = false;
    header->slotNum = 0;
    header->freespace = PAGE_SIZE-sizeof(NonLeafNodeHeader);
    header->ParentNodeId = -1; //not exists yet
    header->startOffset = sizeof(NonLeafNodeHeader);

    //append this nonleaf node into file
    indexFileHandle.appendPage(buffer);
    int pID = indexFileHandle.getNumberOfPages()-1;
    // cout<<"number of pages: "<<pID+1;

    free(buffer);

    return pID;
}

    //get leaf page
    //insert
    //if freespace < new key pair
    //  inLeaf key search : find the correct position for the new key
    //  insert new record and slot
    //else
    //  split
    //      inLeaf key search : find the correct position for the new key
    //      find the middle position
    //      append new leaf node from the middle 
    //      move the larger value to the new page
    //      recursively update the nonleaf node
    //          a pointer in the leafnode points to the nonleafnode
    //          'copy up' the middle key to the non leaf node
    //          if non leaf node overflows
    //              split nonleaf node
    //              similar recursion as above
    //              if root node got split
    //                  update root node
    //      smaller leaf node points to the larger one
    //  
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // 1. search the pageId of the key
    int targetPageId = -3;
    int firstLeafPageId;
    int pageId;
    // cout<<"search value: "<<(char*)key<<endl;

    if(key == NULL)
    {
        return -1;
    }

    targetPageId = ixfileHandle.searchNode(attribute, key);

    // cout << "after search node" << endl;

    // if there is no root page, append a new leaf page as the root node
    if(targetPageId == -1) 
    {
        firstLeafPageId = ixfileHandle.appendLeafNode();
        // cout<<firstLeafPageId<<endl;
        ixfileHandle.setRootPageNum(firstLeafPageId);
        // insert the index to the new root leaf node
        if(ixfileHandle.insertIndexToLeaf(firstLeafPageId, attribute, key, rid))
        {
            return -1;
        }
        
    }
    else
    {
        // cout<<"insert to page: "<<targetPageId<<endl;
        if(ixfileHandle.insertIndexToLeaf(targetPageId, attribute, key, rid))
        {
            return -1;
        }
        // cout << "insert to leaf success" << endl;
    }
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    int pageNum = -1;
    pageNum = ixfileHandle.searchNode(attribute, key);
    if(ixfileHandle.deleteIndexOfLeaf(pageNum, attribute, key, rid))
    {
        return -1;
    }
    return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    // cout << "ixfileHandle.indexFileHandle.getFileName()" << ixfileHandle.indexFileHandle.getFileName() << endl;
    if(ixfileHandle.indexFileHandle.getFileName().empty())
    {
        return -1;
    }
    // cout<<"index filehandle name: "<<ixfileHandle.indexFileHandle.getFileName()<<endl;
    // 1. initialize scanIterator attributes
    ix_ScanIterator.getCondition(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ixfileHandle);

    // 2. search node -> get leaf PageId of lowKey
    //    if lowkey = NULL, start from first page

    int lowKeyPageId = -1;
    int highKeyPageId = -1;
    if(lowKey == NULL)
    {
        lowKeyPageId = 0;
    }
    else
    {
        lowKeyPageId = ixfileHandle.searchNode(attribute, lowKey);
        // cout<<"low key: "<<*(int*)lowKey<<" highkey: "<<*(int*)highKey<<endl;

    }
    if(highKey == NULL)
    {
        highKeyPageId = -1;
    }
    else
    {
        highKeyPageId = ixfileHandle.searchNode(attribute, highKey);
    }

    // cout<<"ffffff"<<endl;


    // 3. search lowKey in leafPage

    unsigned lowKeyLength = 0;
    unsigned highKeyLength = 0;
    void* lowKeyValue;
    void* highKeyValue;
    
    if(attribute.type == TypeVarChar)
    {

        memcpy(&lowKeyLength, lowKey, sizeof(unsigned));
        lowKeyValue = malloc(lowKeyLength);
        memcpy(lowKeyValue, (byte*)lowKey+sizeof(unsigned), lowKeyLength);

        memcpy(&highKeyLength, highKey, sizeof(unsigned));
        highKeyValue = malloc(highKeyLength);
        memcpy(highKeyValue, (byte*)highKey+sizeof(unsigned), highKeyLength);
    }
    else
    {
        lowKeyValue = malloc(sizeof(int));
        highKeyValue = malloc(sizeof(int));
        lowKeyValue = const_cast <void*>(lowKey);
        highKeyValue = const_cast <void*>(highKey);
        
        // memcpy(lowKeyValue, (byte*)lowKey, sizeof(int));
        // memcpy(highKeyValue, (byte*)highKey, sizeof(int));
    }
    
    if(lowKey != NULL)
    {
        void* buffer1 = malloc(PAGE_SIZE);
        if(ixfileHandle.indexFileHandle.readPage(lowKeyPageId, buffer1))
        {
            cout << "scan readPage fail, page num:"<< lowKeyPageId << endl;
            return -1;
        }

        short startSlot = 0;
        LeafNodeHeader* header = (LeafNodeHeader*)buffer1;

        leafSlot* tempSlot = ixfileHandle.inLeafNodeInsertSearch(lowKeyInclusive, buffer1, header, attribute, startSlot, lowKeyValue, lowKeyLength);
        // cout << "startSlot " << startSlot <<  endl;
        if(startSlot == header->slotNum){
            // cout<<"found nothing"<<endl;
            return -1;
        }
        ix_ScanIterator.startPage = lowKeyPageId;
        ix_ScanIterator.startSlot = startSlot;
        free(buffer1);
    }
    else
    {
        ix_ScanIterator.startPage = 0;
        ix_ScanIterator.startSlot = 1;
    }
    if(highKey != NULL)
    {
        // 4. search highKey in leafPage
        void* buffer2 = malloc(PAGE_SIZE);
        if(ixfileHandle.indexFileHandle.readPage(highKeyPageId, buffer2))
        {
            cout << "scan readPage fail, page num:"<< highKeyPageId << endl;
            return -1;
        }

        short endSlot = 0;
        LeafNodeHeader* header2 = (LeafNodeHeader*)buffer2;
        
        leafSlot* tempSlot = ixfileHandle.inLeafNodeInsertSearch(!highKeyInclusive, buffer2, header2, attribute, endSlot, highKeyValue, highKeyLength);
        ix_ScanIterator.endPage = highKeyPageId;
        // cout << "endSlot" << endSlot << endl;
        ix_ScanIterator.endSlot = endSlot;
        // cout<<"end slot: "<<endSlot<<endl;
        free(buffer2);
    }
    else
    {
        ix_ScanIterator.endPage = -1;
        ix_ScanIterator.endSlot = -1;
    }
    // cout << "step 1" << endl;

    void* slotBuffer = malloc(PAGE_SIZE);
    // cout << "lowKeyId: " << lowKeyPageId << endl;
    // cout <<"number of Page: "<< ixfileHandle.indexFileHandle.getNumberOfPages() << endl;

    if(ixfileHandle.indexFileHandle.readPage(lowKeyPageId, slotBuffer))
    {
        cout << "readPage fail" << endl;
        return -1;
    }
    // cout << "step 2" << endl;

    LeafNodeHeader * header = (LeafNodeHeader*)slotBuffer;
    ix_ScanIterator.curNumOfSlot = header->slotNum;
    free(slotBuffer);

    ix_ScanIterator.curPage = ix_ScanIterator.startPage;
    ix_ScanIterator.curSlot = ix_ScanIterator.startSlot;
    // cout << "start page:slot" << ix_ScanIterator.startPage<<":"<<ix_ScanIterator.startSlot << endl;
    // cout << "end page:slot" << ix_ScanIterator.endPage<<":"<<ix_ScanIterator.endSlot << endl;

    // free(lowKeyValue);
    // free(highKeyValue);
    // ix_ScanIterator.setStartEnd(lowKeyPageId, highKeyPageId, startSlot, endSlot);
    return 0;
}

RC printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum){
    void* buffer = malloc(PAGE_SIZE);
    if(ixfileHandle.indexFileHandle.readPage(pageNum, buffer) == -1){
        free(buffer);
        return -1;
    }
    bool* leafCheck = (bool*)buffer;
    if(*leafCheck == true){
        //this is a leaf node
        LeafNodeHeader* header = (LeafNodeHeader*)buffer;
        leafSlot* tempslot = (leafSlot*)((byte*)buffer + PAGE_SIZE - sizeof(leafSlot));
        cout<<"{"<<"\"keys\":[";
        for(int i = 0; i<header->slotNum; i++){
            //print the value one by one
            byte* keyValue = (byte*)buffer + tempslot->offset;
            if(attribute.type == TypeVarChar){
                char* tempChar = (char*)malloc(attribute.length);
                memset(tempChar,0,attribute.length);

                int* keyLength = (int*)keyValue;
                keyValue += 4;
                memcpy(tempChar,keyValue,*keyLength);
                keyValue += *keyLength;
                RID* tempRID = (RID*)keyValue;

                cout<<"\""<<tempChar<<":"<<"("<<tempRID->pageNum<<","<<tempRID->slotNum<<")\"";
                free(tempChar);
            }
            else if(attribute.type == TypeInt){
                cout<<"\""<<*(int*)keyValue<<":";
                keyValue += 4;
                RID* tempRID = (RID*)keyValue;
                cout<<"("<<tempRID->pageNum<<","<<tempRID->slotNum<<")\"";
            }
            else if(attribute.type == TypeReal){
                cout<<"\""<<*(float*)keyValue<<":";
                keyValue += 4;
                RID* tempRID = (RID*)keyValue;
                cout<<"("<<tempRID->pageNum<<","<<tempRID->slotNum<<")\"";
            }
            if(i == header->slotNum-1)
                break;
            cout<<",";
            tempslot--;
        }
        cout<<"]";
        cout<<"}";
    }
    else{
        //this is a non leaf node
        NonLeafNodeHeader* header = (NonLeafNodeHeader*)buffer;
        vector<int> childNodes;
        cout<<"{"<<"\"keys\":";
        nonleafSlot* tempslot = (nonleafSlot*)((byte*)buffer + PAGE_SIZE - sizeof(nonleafSlot));

        cout<<"[";
        for(unsigned i = 0; i< header->slotNum; i++){
            //print key one by one 
            //stored the child nodes into vector
            byte* keyValue = (byte*)buffer + tempslot->offset;
            if(attribute.type == TypeVarChar){
                char* tempChar = (char*)malloc(attribute.length);
                memset(tempChar,0,attribute.length);
                memcpy(tempChar,keyValue,tempslot->klength);
                cout<<"\""<<tempChar<<"\"";
                free(tempChar);
            }
            else if(attribute.type == TypeInt){
                cout<<"\""<<*(int*)keyValue<<"\"";
            }
            else if(attribute.type == TypeReal){
                cout<<"\""<<*(float*)keyValue<<"\"";
            }

            int* leftPtr = (int*)(keyValue-4);
            childNodes.push_back(*leftPtr);

            //for the last one, store the right pointer in the vector
            if(i == header->slotNum-1){
                int* rightPtr = (int*)(keyValue+tempslot->klength);
                childNodes.push_back(*rightPtr);
                break;
            }
            cout<<",";
            tempslot--;
        }
        cout<<"],";
        cout<<"\"children\":[";
        for(int i = 0; i<childNodes.size(); i++){
            printNode(ixfileHandle,attribute,childNodes[i]);
            if(i == childNodes.size()-1)
                break;
            cout<<",";
        }
        cout<<"]";
        cout<<"}";
        cout<<endl<<childNodes.size()<<endl;
    }
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    int rootNumber = ixfileHandle.getRootPageNum();
    cout << endl;
    printNode(ixfileHandle, attribute, rootNumber);
}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::setStartEnd(int pageStart, int pageEnd, int startSlot, int endSlot)
{
    this->startPage = pageStart;
    this->endPage = pageEnd;
    this->curPage = pageStart;
    this->curSlot = startSlot;
    this->startSlot = startSlot;
    this->endSlot = endSlot;
}


RC IX_ScanIterator::getCondition(Attribute attribute, const void *lowKey, 
                                  const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, IXFileHandle &ixFileHandle)
{
    this->attribute = attribute;
    this->lowKey = lowKey;
    this->highKey = highKey;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;
    this->ixFileHandle = &ixFileHandle;
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    // if not high key
    // cout << "curPage " << curPage << endl;
    // cout << "endPage " << endPage << endl;
    // cout << "curSlot " << curSlot << endl;
    // cout << "endSlot " << endSlot << endl;


    // if reach the last leaf node
    if(curPage == -1)
    {
        return IX_EOF;
    }
    
    // if reach the high key
    else
    {
        if(curPage == endPage && curSlot > endSlot)
        {
            return IX_EOF;
        }
    }
    
    // Read the key and rid
    void* buffer = malloc(PAGE_SIZE);
    // cout << "next Entry request page: " << curPage << endl;
    // cout<<"ending slot: "<<endSlot<<endl;
    if(ixFileHandle->indexFileHandle.readPage(curPage, buffer))
    {
        cout << "readPage fail" << endl;
        return -1;
    }
    LeafNodeHeader * header = (LeafNodeHeader*)buffer;

    // cout << "lastNumofSlot" << curNumOfSlot << endl;
    // cout << "curNumberOfSlot" << header->slotNum << endl;
    if( header->slotNum < curNumOfSlot)
    {
        curSlot -= (curNumOfSlot-header->slotNum);
        // cout << "update curSlot : " << curSlot << endl;
        curNumOfSlot = header->slotNum;
    }
    leafSlot* tempSlot = (leafSlot*) ((char*)buffer + PAGE_SIZE - sizeof(leafSlot)*curSlot);


    int keyLength = 0;
    if(attribute.type == TypeVarChar)
    {
        // copy the first 4 bytes of a indexPairData which is the length of key to the comparedKeyLength
        memcpy(&keyLength, (char*)buffer + (tempSlot->offset), sizeof(int));
        // copy the varchar key of a indexPairData to the comparedKey
        memcpy(key, (char*)buffer + (tempSlot->offset) + sizeof(int), keyLength);
        // copy the rid into give variable
        memcpy(&rid, (char*)buffer + (tempSlot->offset) + sizeof(int) + keyLength, sizeof(RID));
    }
    else
    {
        memcpy(key, (char*)buffer + (tempSlot->offset), sizeof(int));
        // cout << "tempSlot offset" <<tempSlot->offset<< endl;

        memcpy(&rid, (char*)buffer + (tempSlot->offset) + sizeof(int), sizeof(RID));
    }


    // update curSlot and curPage
    curSlot++;
    if(curSlot > header->slotNum)
    {
        curPage = header->nextLeafId;
            // Read the key and rid
        if(curPage > -1)
        {
            void* slotBuffer = malloc(PAGE_SIZE);
            // cout << "next Entry request page: " << curPage << endl;
            if(ixFileHandle->indexFileHandle.readPage(curPage, slotBuffer))
            {
                cout << "readPage fail" << endl;
                return -1;
            }
            LeafNodeHeader * header = (LeafNodeHeader*)slotBuffer;
            curNumOfSlot = header->slotNum;
            free(slotBuffer);
        }
        
        curSlot = 1;
    }


    free(buffer);

    return 0;
}

RC IX_ScanIterator::close()
{
    // free((void*)lowKey);
    // free((void*)highKey);
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}


RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    indexFileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    return 0;
}

// return -1: not rootPage, return other>-1 means rootPageNum
int IXFileHandle::getRootPageNum()
{
    
    int rootPageNum = -1;
    rootPageNum = indexFileHandle.getRootpageNum();
    // cout << "getRootNumber" << rootPageNum << endl;

    return rootPageNum;

}

RC IXFileHandle::setRootPageNum(int rootPageNum)
{
    if(indexFileHandle.setRootPageNum(rootPageNum))
    {
        return -1;
    }
    return 0;
}

