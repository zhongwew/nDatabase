#include "rbfm.h"
#include <cstring>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

unsigned RecordBasedFileManager::getWriteAppendCounterValue(FileHandle &handle)
{
    unsigned readCounter, writeCounter, appendCounter;

    PagedFileManager * pfm = PagedFileManager::instance();
    handle.collectCounterValues(readCounter, writeCounter, appendCounter);
    return writeCounter+appendCounter;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager * pfm = PagedFileManager::instance();
    //call the pfm to create a new paged file
    if(pfm->createFile(fileName) != 0)
        return -1;
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager * pfm = PagedFileManager::instance();
    if(pfm->destroyFile(fileName) != 0)
        return -1;
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    //pass the filename and filehandle to open it
    PagedFileManager * pfm = PagedFileManager::instance();

    if(pfm->openFile(fileName,fileHandle) != 0)
        return -1;
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    //pass the filehandle the pagedsystem to close it
    PagedFileManager * pfm = PagedFileManager::instance();
    if(pfm->closeFile(fileHandle) != 0)
        return -1;
    return 0;
}

struct slot{
    //offset equals -1 means the slot is empty
    short offset;//the offset of record
    short rLeng;//record length
    bool rFlag = false;//indicate if the record is a rid
};

struct pageHeader{
    short freespace;
    short slotnum;
    short startoffset; //record where to insert new record
};

void* recordAnalysis(const vector<Attribute> &recordDescriptor, const void* data, int& rLength){

    int rNum = recordDescriptor.size();
    int nullbytenum = ceil((double)rNum/8);

    //nullindicator indicate which field is null
    //data ptr points to the real data
    byte* nullindicator = (byte*)data;
    byte* dataptr = (byte*)data + nullbytenum;

    void* buffer = malloc(PAGE_SIZE);

    int offset = 0; //used to track offset in record

    byte* length = (byte*)buffer + offset;
    *length = rNum;
    offset += 1;//move 1 byte

    byte* nptr = (byte*)buffer + offset;
    memcpy(nptr,nullindicator,nullbytenum);
    offset += nullbytenum;

    short* valuelength = (short*)((byte*)buffer + offset);
    offset += rNum*2;

    //copy the value from raw data to buffer one by one
    for(int i = 0; i < rNum; i++){
        //check if it's null
        byte* nTemp = nptr + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));

        //determine what kind of data it is
        if(recordDescriptor[i].type == TypeInt){
            if(nBit){
                //move to next value
                valuelength++;
                continue;
            }
            byte* intptr = (byte*)buffer + offset;
            memcpy(intptr,dataptr,recordDescriptor[i].length);
            offset += recordDescriptor[i].length;
            //move the raw data pointer
            dataptr += recordDescriptor[i].length;
            //record the offset
            *valuelength = (short)offset;
            rLength = offset;
            valuelength ++;
        }
        else if(recordDescriptor[i].type == TypeReal){
            if(nBit){
                //move to next value
                valuelength++;
                continue;
            }
            byte* realptr = (byte*)buffer + offset;
            memcpy(realptr,dataptr,recordDescriptor[i].length);
            offset += recordDescriptor[i].length;
            //move the raw data pointer
            dataptr += recordDescriptor[i].length;
            //record the offset
            *valuelength = (short)offset;
            rLength = offset;
            valuelength ++;
        }
        else if(recordDescriptor[i].type == TypeVarChar){
            if(nBit){
                //move to next value
                valuelength++;
                continue;
            }
            //get the length of varchar first
            int * lengPtr = (int*)dataptr;
            dataptr += 4;

            //get the value then
            byte* varptr = (byte*)buffer + offset;
            memcpy(varptr,dataptr,*lengPtr);
            offset += *lengPtr;
            //move the raw data pointer
            dataptr += *lengPtr;
            //record the offset
            *valuelength = (short)offset;
            rLength = offset;
            valuelength ++;
        }
    }
return buffer;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //analysis the record first
    void* recorddata;
    int rLength;
    recorddata = recordAnalysis(recordDescriptor,data, rLength);
    void* buffer = malloc(PAGE_SIZE);

    //get current page counters
    unsigned pagecounter = 0;
    //examine from the first page to find place to insert record
    while(pagecounter < fileHandle.getNumberOfPages()){
        fileHandle.readPage(pagecounter,buffer);
        //check free space in this page
        pageHeader * header = (pageHeader*)buffer;
        if(header->freespace < rLength+sizeof(slot)){
            pagecounter++;
            continue;
        }
        //go through slot directory to find empty slot
        bool emptyb = false;
        slot * tempslot = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
        for(int j = 0; j<(int)header->slotnum; j++){
            if(tempslot->offset == -1){
                //find an empty slot
                tempslot->offset = header->startoffset;
                tempslot->rLeng = rLength;
                tempslot->rFlag = false;
                emptyb = true;
                //update header
                header->freespace -= rLength;
                //record rid
                rid.slotNum = j;
                rid.pageNum = pagecounter;
                break;
            }
            tempslot--;
        }
        //if no empty slot, create one
        if(!emptyb){
            tempslot->offset = header->startoffset;
            tempslot->rLeng = rLength;
            tempslot->rFlag = false;
            //update header
            header->freespace -= (rLength+sizeof(slot));
            header->slotnum++;
            //record rid
            rid.slotNum = header->slotnum-1;//the last one
            rid.pageNum = pagecounter;
        }
        //insert new data
        memcpy((char*)buffer+header->startoffset,recorddata,rLength);
        header->startoffset += rLength;
        //write back to file
        fileHandle.writePage(pagecounter,buffer);
        free(recorddata);
        free(buffer);
        return 0;
    }

    //need to append a new page using buffer
    //the header of page include two integer: 
    memset(buffer,0,PAGE_SIZE);
    //add the header and slot directory to the new page
    slot * slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    pageHeader * ph = (pageHeader*)buffer;
    ph->freespace = PAGE_SIZE-sizeof(pageHeader)-sizeof(slot);
    ph->slotnum = 1;
    void * recordstart = (char*)buffer + sizeof(pageHeader);

    memcpy(recordstart,recorddata,rLength);

    //update the start of insert offset
    ph->startoffset = sizeof(pageHeader)+rLength;
    ph->freespace -= rLength;

    slotcell->offset = sizeof(pageHeader);
    slotcell->rLeng = rLength;
    slotcell->rFlag = false;
    //append the page to file
    fileHandle.appendPage(buffer);

    rid.pageNum = pagecounter;
    rid.slotNum = 0;

    free(buffer);
    free(recorddata);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void * buffer = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,buffer) == -1) 
    {
        // cout << "read page: " << rid.pageNum << " fail" << endl;
        return -1;
    }

    //find the corresponding slotnum
    slot* slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    slotcell -= rid.slotNum;

    //reading a deleted slot should return fail
    if(slotcell->offset == -1){

        free(buffer);
        return -1;
    }
    if(slotcell->rFlag){
        //the stored record is a rid
        RID* nrid = (RID*)((byte*)buffer+slotcell->offset);
        if(readRecord(fileHandle, recordDescriptor, *nrid,data) != 0) return -1;
        free(buffer);
        return 0;
    }

    //find the offset according to the slot
    void* readdata = (char*)buffer + slotcell->offset;
    
    //format conversion
    //used for readdata and data respectively
    int internalOffset = 0;
    int outputOffset = 0;

    //skip the field length one
    internalOffset += 1; 

    //read the null indicator
    byte* nullIndicator = (byte*)readdata + internalOffset;
    byte* outputNull = (byte*)data + outputOffset;
    int nullNum = ceil((double)recordDescriptor.size()/8);
    memcpy(outputNull,nullIndicator,nullNum);

    internalOffset += nullNum;
    outputOffset += nullNum;

    //set a pointer to point offset field
    short* offsetPtr = (short*)((byte*)readdata + internalOffset);
    internalOffset += recordDescriptor.size()*2;

    for(int i = 0; i<recordDescriptor.size(); i++){
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit){
            offsetPtr++;
            continue;
        }
        if(recordDescriptor[i].type == TypeInt){
            byte* intptr = (byte*)readdata + internalOffset;
            memcpy((byte*)data+outputOffset,intptr,recordDescriptor[i].length);
            outputOffset += recordDescriptor[i].length;
            internalOffset += recordDescriptor[i].length;
            offsetPtr++;
        }
        else if(recordDescriptor[i].type == TypeReal){
            byte* floatptr = (byte*)readdata + internalOffset;
            memcpy((byte*)data+outputOffset,floatptr,recordDescriptor[i].length);
            outputOffset += recordDescriptor[i].length;
            internalOffset += recordDescriptor[i].length;
            offsetPtr++;
        }
        else if(recordDescriptor[i].type == TypeVarChar){
            //get the length first
            int varLength = *offsetPtr - internalOffset;
            //store the length before the value
            int* varLengPtr = (int*)((byte*)data + outputOffset);
            *varLengPtr = varLength;
            //move the output data pointer
            outputOffset += 4;
            //copy the varchar value
            byte* varptr = (byte*)readdata + internalOffset;
            memcpy((byte*)data+outputOffset,varptr,varLength);
            outputOffset += varLength;
            internalOffset += varLength;
            offsetPtr++;

        }
    }
    free(buffer);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    //print the corresponding data
    int offset = 0;

    //read the nullIndicator
    byte* nullIndicator = (byte*)data + offset;
    int nullNum = ceil((double)recordDescriptor.size()/8);
    offset += nullNum;

    for(int i = 0; i<recordDescriptor.size(); i++){
        //check the null pointer
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit){
            //print info
            cout<< recordDescriptor[i].name<<":"<<"NULL"<<"\t";
            continue;
        }
        if(recordDescriptor[i].type == TypeInt){
            int* intvalue = (int*)((char*)data + offset);
            cout<<recordDescriptor[i].name<<":"<<*intvalue<<"\t";
            //move the offset
            offset += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeReal){
            float* floatvalue = (float*)((char*)data + offset);
            cout<<recordDescriptor[i].name<<":"<<*floatvalue<<"\t";
            //move the offset
            offset += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeVarChar){
            //check the varchar length
            int* charlength = (int*)((char*)data+offset);
            // cout << "charLen: " << *charlength<< endl;
            offset += 4;

            //output the varchar
            char* charvalue = (char*)data + offset;
            char* outputstr = (char*)malloc(*charlength+1);
            memcpy(outputstr,charvalue,*charlength);
            cout<<recordDescriptor[i].name<<":"<<outputstr<<"\t";
            free(outputstr);
            offset += *charlength;
        }
    }
    return 0;
}

//compact the records and leave the middle freespace
//direction:1(left)/0(right)
void compactRecord(void* buffer, int startOffset, int endOffset, int moveLength, int direction){
    //define the slotcell and page header
    slot* slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    pageHeader * ph = (pageHeader*)buffer;

    //the start record offset we are looking for
    short tempOffset = startOffset;
    while(tempOffset != endOffset){
        slot* tempslot = slotcell;
        for(int i = 0; i<ph->slotnum; i++){
            if(tempslot->offset == tempOffset && direction == 1){
                //the record move to left
                memmove((byte*)buffer+tempslot->offset-moveLength,(byte*)buffer+tempslot->offset,tempslot->rLeng);
                //update the slot info
                tempslot->offset -= moveLength;
                //update the offset to locate the next record to move
                tempOffset += tempslot->rLeng;
                break;
            }
            else if(tempslot->offset + tempslot->rLeng == tempOffset && direction == 0){
                //the record move to right
                memmove((byte*)buffer+tempslot->offset+moveLength,(byte*)buffer+tempslot->offset,tempslot->rLeng);
                //update the slot info
                tempslot->offset += moveLength;
                //update the offset to locate the next record to move
                tempOffset -= tempslot->rLeng;
                break;
            }
            tempslot--;
        }
    }
    if(direction){
        ph->startoffset -= moveLength;
        ph->freespace += moveLength;
    }
    else{
        ph->startoffset += moveLength;
        ph->freespace -= moveLength;
    }

}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    //locate the record according to rid
    // cout<<"deleting rid: "<<rid.pageNum<<"\t"<<rid.slotNum<<endl;
    void * buffer = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,buffer) == -1)   return -1;
     //find the corresponding slotnum

    slot* slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    slot* targetcell = slotcell - rid.slotNum;
    pageHeader * ph = (pageHeader*)buffer;


    if(targetcell->offset == -1){
        //can't delete an already deleted record
        free(buffer);
        return -1;
    }
    if(targetcell->rFlag){
        //the stored record is a rid
        const RID nrid = *(RID*)((byte*)buffer+targetcell->offset);
        // cout<<"new rid is: "<<nrid.pageNum<<"\t"<<nrid.slotNum<<endl;
        if(deleteRecord(fileHandle, recordDescriptor, nrid) != 0) return -1;
        //move the other records
        compactRecord(buffer, targetcell->offset+sizeof(RID),ph->startoffset, sizeof(RID),1);
        //mark the corredponding slot as -1
        targetcell->offset = -1;
        targetcell->rLeng = -1;
        targetcell->rFlag = false;
        free(buffer);
        return 0;
    }


    //get the corresponding offset and length
    short rOffset = targetcell->offset;
    short rLength = targetcell->rLeng;

    //mark the corredponding slot as -1
    targetcell->offset = -1;
    targetcell->rLeng = -1;

    //move the other records
    compactRecord(buffer, rOffset+rLength,ph->startoffset, rLength,1);

    //write the page back
    if(fileHandle.writePage(rid.pageNum,buffer) == -1)    return -1;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    //read the corresponding page
    void * buffer = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,buffer) != 0)    return -1;

    //find the corresponding slotnum
    slot* slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    slot* targetcell = slotcell - rid.slotNum;
    if(targetcell->offset == -1){
        //it's wrong to update a empty record
        free(buffer);
        return -1;
    }
    if(targetcell->rFlag){
        //the stored record is a rid
        const RID nrid = *(RID*)((byte*)buffer+targetcell->offset);
        if(updateRecord(fileHandle, recordDescriptor, data, rid) != 0) return -1;
        free(buffer);
        return 0;
    }

    //get the corresponding offset and length
    short rOffset = targetcell->offset;
    short rLength = targetcell->rLeng;
    byte * oRecord = (byte*)buffer + rOffset;

    //get the new record
    int nrLength;
    void * nRecord = recordAnalysis(recordDescriptor,data,nrLength);

    pageHeader* ph = (pageHeader*)buffer;


    if(nrLength-rLength < ph->freespace){
        //if the new record is shorter, compact the space
        //copy the new record
        if(nrLength > rLength){
            //new record is larger, need to move record to right
            compactRecord(buffer, ph->startoffset, rOffset+rLength, nrLength-rLength, 0);
            memcpy(oRecord,nRecord,nrLength);
            targetcell->rLeng = nrLength;
            free(nRecord);
        }
        else{
            //new record is smaller, need to move record to left
            memcpy(oRecord,nRecord,nrLength);
            targetcell->rLeng = nrLength;
            //compact the space
            compactRecord(buffer,rOffset+rLength,ph->startoffset, rLength-nrLength,1);
            free(nRecord);
        }
    }
    else{
        //if the new record is larger than freespace, insert the new record to a new page
        RID nrid;
        insertRecord(fileHandle, recordDescriptor,data,nrid);
        //insert the new rid as a tombstone
        targetcell->rFlag = true;
        //copy the rid to the record's offset
        memcpy(oRecord,&nrid,sizeof(RID));
        //update the slot
        targetcell->rLeng = sizeof(RID);
        //compact the freespace
        compactRecord(buffer,rOffset+rLength,ph->startoffset, rLength-sizeof(RID), 1);
        if(((RID*)oRecord)->pageNum != nrid.pageNum || ((RID*)oRecord)->slotNum != nrid.slotNum) exit(0);
        free(nRecord);
    }
    if(fileHandle.writePage(rid.pageNum,buffer) != 0)   return -1;
    free(buffer);
    return 0;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    //read the corresponding page
    void * buffer = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,buffer) != 0)    return -1;

    //find the corresponding slotnum
    slot* slotcell = (slot*)((char*)buffer+PAGE_SIZE-sizeof(slot));
    slot* targetcell = slotcell - rid.slotNum;
    if(targetcell->offset == -1){
        //can't delete an already deleted record
        free(buffer);
        return -1;
    }
    if(targetcell->rFlag){
        //the stored record is a rid
        const RID nrid = *(RID*)((byte*)buffer+targetcell->offset);
        // cout<<"new rid is: "<<nrid.pageNum<<"\t"<<nrid.slotNum<<endl;
        if(readAttribute(fileHandle, recordDescriptor, nrid,attributeName,data) != 0) return -1;
        free(buffer);
        return 0;
    }

    // cout<<"start reading"<<endl;
    int attrNum = 0;
    int nullbytenum = ceil((double)recordDescriptor.size()/8);
    bool strFlag = false;

    //figure out which attribute are we looking for
    for(int i = 0; i < recordDescriptor.size(); i++)
        if(recordDescriptor[i].name == attributeName){
            attrNum = i;
            if(recordDescriptor[i].type == TypeVarChar)
                strFlag = true;
            break;
        }
    //find the record
    byte* record = (byte*)buffer + targetcell->offset;
    //define a counter to locate different attribute
    int offset = 0;
    int dOffset = 0;

    //pass the first byte
    offset += 1;

    //copy the nullindicator
    byte* nullIndicator = record + offset;
    byte oNull = 0;
    //judge if it's null value
    bool ifNull = (*(nullIndicator+attrNum/8)) & (1<<(7-attrNum));
    if(ifNull){
        oNull = 1<<7;
        // cout<<"it's null"<<endl;
        // cout<<"attrnum "<<attrNum<<endl;
    }
    memcpy(data,&oNull,1);
    offset += nullbytenum;
    dOffset += 1;
    if(ifNull) return 0;

    //get the attribute's ending offset which is the previous's ending offset
    offset += attrNum*2;
    short * eaOffset = (short*)(record + offset);
    if(attrNum == 0){
        int startOffset = recordDescriptor.size()*2+1+nullbytenum;
        if(strFlag){
            int vLength = *eaOffset-startOffset;
            memcpy((byte*)data+dOffset,&vLength,4);
            dOffset += 4;
        }
        memcpy((byte*)data+dOffset,record+startOffset,*eaOffset-startOffset);
    }
    else{
        //if it's not the first attribute
        short * saOffset = (short*)(record + offset-2);
        if(strFlag){
            int vLength = *eaOffset-*saOffset;
            memcpy((byte*)data+dOffset,&vLength,4);
            dOffset += 4;
        }
        memcpy((byte*)data+dOffset,record+*saOffset,*eaOffset-*saOffset);
    }
    free(buffer);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator){
          //store the filter into iterator
          rbfm_ScanIterator.attributeNames = attributeNames;
          rbfm_ScanIterator.compOp = compOp;
        // cout << "record descriptor size: " << recordDescriptor.size() << endl;
    
        //figure out what is the constraint attribute
        rbfm_ScanIterator.cAttrIndex = -1;
        // cout<<"looking for name: "<<conditionAttribute<<endl;
        for(int i = 0; i < recordDescriptor.size(); i++)
        {
            // cout<<"rd name: "<<recordDescriptor[i].name<<endl;
            if(recordDescriptor[i].name == conditionAttribute){
                // cout << "Match: " << recordDescriptor[i].name << endl;
                rbfm_ScanIterator.cAttr = recordDescriptor[i];
                // cout << recordDescriptor[i].name << endl;
                rbfm_ScanIterator.cAttrIndex = i;
                // cout<<"cattrIndex: "<<i<<endl;
                break;
            }
        }
        // cout << "end load scan iterator" << endl;
          rbfm_ScanIterator.value = value;
          rbfm_ScanIterator.fileHandle = fileHandle;
          rbfm_ScanIterator.recordDescriptor = recordDescriptor;

          //set the current pageId as default value
          rbfm_ScanIterator.curRid.pageNum = 0;
          rbfm_ScanIterator.curRid.slotNum = 0;
          return 0;
      }

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    //get the total pagenumber
    // cout<<endl<<fileHandle.getFileName()<<endl;
    int pageNum = fileHandle.getNumberOfPages();
    // cout<<"page number is: "<<pageNum;
    while(curRid.pageNum < pageNum){
        // cout << "rid.PageNum" << curRid.pageNum << endl;
        // cout << "pageNum" << pageNum << endl;

        //if the slot num equals to 0, open a new page
        if(curRid.slotNum == 0){
            //if it's page 0, there's no need to deallocate
            if(curRid.pageNum != 0)
                free(curPage);
            //buffer a new page
            curPage = malloc(PAGE_SIZE);
            fileHandle.readPage(curRid.pageNum,curPage);
        }

        //find the corresponding slot
        slot* slotcell = (slot*)((char*)curPage+PAGE_SIZE-sizeof(slot));
        slot* targetcell = slotcell - curRid.slotNum;

        //get the page header
        pageHeader * ph = (pageHeader*)curPage;

        rid = curRid;
        //move to next slot
        curRid.slotNum += 1;
        if(curRid.slotNum == ph->slotnum){
            //need to turn to next page
            curRid.pageNum += 1;
            curRid.slotNum = 0;
        }
        if(targetcell->offset == -1 || targetcell->rFlag == true)
            continue;
        //check if current record is valid, if true, return it
        byte* record = (byte*)curPage + targetcell->offset;
        if(isValid(record)){
            extractRecord(record, data);
            return 0;
        }
    }
    // cout<<"finish get next rbmf"<<endl;

    return RBFM_EOF;
}

bool RBFM_ScanIterator::isValid(void * record){
    if(cAttrIndex == -1) return true;
    int nullbytenum = ceil((double)recordDescriptor.size()/8);

    //define a counter to locate different attribute
    int offset = 0;
    //pass the first byte
    offset += 1;

    byte* nullIndicator = (byte*)record + offset;
    offset += nullbytenum;

    bool ifNull = *(nullIndicator+cAttrIndex/8) & (1<<(7-cAttrIndex));
    if(ifNull) return false;



    //get the attribute's ending offset which is the previous's ending offset
    offset += cAttrIndex*2;

    short * stOffset = (short*)malloc(sizeof(short));
    short * eaOffset = (short*)((byte*)record + offset);

    if(cAttrIndex == 0){
        *stOffset = recordDescriptor.size()*2+1+nullbytenum;
    }
    else{
        //if it's not the first attribute
        stOffset = (short*)((byte*)record + offset-2);
    }

    short attrLength;
    const void* tempValue = value;

    //type conversion
    if(cAttr.type == TypeInt){
        tempValue = (int*)value;
        attrLength = 4;
        record = (int*)((byte*)record+*stOffset);
    }
    else if(cAttr.type == TypeReal){
        tempValue = (float*)value;
        attrLength = 4;
        record = (float*)((byte*)record+*stOffset);
    }
    else if(cAttr.type == TypeVarChar){
        attrLength = *(int*)value;
        tempValue = (char*)value+4;
        record = (char*)((byte*)record+*stOffset);
    }
    //do the comparision
    switch(compOp){
        case EQ_OP:
            if(memcmp(tempValue,record,attrLength) == 0)
                return true;
            else
                return false;
            break;
        case LT_OP:
            if(memcmp(record,tempValue,attrLength) < 0)
                return true;
            else
                return false;
            break;
        case LE_OP:
            if(memcmp(record,tempValue,attrLength) <= 0)
                return true;
            else
                return false;
            break;
        case GT_OP:
            if(memcmp(record,tempValue,attrLength) > 0)
                return true;
            else
                return false;
            break;
        case GE_OP:
            if(memcmp(record,tempValue,attrLength) >= 0)
                return true;
            else
                return false;
            break;
        case NE_OP:
            if(memcmp(record,tempValue,attrLength) == 0)
                return false;
            else
                return true;
            break;
        case NO_OP:
            return true;
        default:
            break;
    }
    return 0;
}

RC RBFM_ScanIterator::extractRecord(void* record, void *data){
    //extract the request attribute from record to data
        
    //format conversion
    //used for readdata and data respectively
    int internalOffset = 0;
    int outputOffset = 0;

    //skip the field length one
    internalOffset += 1; 

    //read the null indicator
    byte* nullIndicator = (byte*)record + internalOffset;
    byte* outputNull = (byte*)data + outputOffset;
    
    //since the selected attributes are different, the number of null byte is different
    int nullNum = ceil((double)recordDescriptor.size()/8);
    int enullNum = ceil((double)attributeNames.size()/8);
    memset(outputNull,0,enullNum);

    internalOffset += nullNum;
    outputOffset += enullNum;

    //set a pointer to point offset field
    short* offsetPtr = (short*)((byte*)record + internalOffset);
    internalOffset += recordDescriptor.size()*2;

    for(int i = 0; i<attributeNames.size(); i++){
        //find the corresponding index in recordDescriptor

        int index;
        for(int j = 0; j<recordDescriptor.size(); j++)
            if(recordDescriptor[j].name == attributeNames[i]){
                index = j;
                break;
            }

        byte* nTemp = nullIndicator + (index/8);
        byte* onTemp = outputNull + (i/8);
        
        //if this record is null in original format
        bool nBit = *nTemp & (1<<(7-index%8));
        if(nBit){
            //mark it as null in new format
            *onTemp = *onTemp | (1<<(7-i%8));
            continue;
        }
        //find the start offset of attribute in internal record

        short* eOffset = offsetPtr + index;
        if(index == 0)
            internalOffset = 1+nullNum+2*recordDescriptor.size();
        else{
            //get the start offset from previous one
            if(index == 1 && (*nullIndicator & (1<<7)))
                internalOffset = 1+nullNum+2*recordDescriptor.size();
            else
                internalOffset = *(eOffset - 1);
        }
        //cout<<"int offset:"<<internalOffset<<"\t"<<*eOffset<<endl;

        if(recordDescriptor[index].type == TypeInt){
            byte* intptr = (byte*)record + internalOffset;
            memcpy((byte*)data+outputOffset,intptr,4);
            outputOffset += 4;
        }
        else if(recordDescriptor[index].type == TypeReal){
            byte* floatptr = (byte*)record + internalOffset;
            memcpy((byte*)data+outputOffset,floatptr,4);
            outputOffset += 4;
        }
        else if(recordDescriptor[index].type == TypeVarChar){
            //get the length first
            int varLength = *eOffset - internalOffset;
            //store the length before the value
            int* varLengPtr = (int*)((byte*)data + outputOffset);
            *varLengPtr = varLength;
            //move the output data pointer
            outputOffset += 4;

            //copy the varchar value
            byte* varptr = (byte*)record + internalOffset;
            memcpy((byte*)data+outputOffset,varptr,varLength);
            outputOffset += varLength;

        }
    }
    return 0;
}


RC RBFM_ScanIterator::close(){
    if(curRid.pageNum != 0 && curRid.slotNum != 0)
        free(curPage);
    return 0;
}