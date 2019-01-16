
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition){
    this->input = input;
    this->cond = condition;
    input->getAttributes(attrs);
    // cout<<"attrs size: "<<attrs.size()<<endl;

    //find the position and offset of compared attribute
    for(int i = 0; i<attrs.size(); i++){
        if(attrs[i].name == cond.lhsAttr)
            lIndex = i;
        else if(cond.bRhsIsAttr && attrs[i].name == cond.rhsAttr)
            rIndex = i;
    }
    // cout << "filter init end"  << endl;
}

bool Filter::condJudge(Value lvalue, Value rvalue, int lIndex, int rIndex){
    switch(cond.op){
        case EQ_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) == 0)
                    return true;
                else
                    return false;
            break;
        case LT_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) < 0)
                    return true;
                else 
                    return false;
            break;
        case LE_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) > 0)
                    return false;
                else 
                    return true;
            break;
        case GT_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) > 0)
                    return true;
                else 
                    return false;
            break;
        case GE_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) < 0)
                    return false;
                else 
                    return true;
            break;
        case NE_OP:
                if(compareValue(lvalue, rvalue, lIndex, rIndex) == 0)
                    return false;
                else 
                    return true;
            break;
        case NO_OP:
            return true;

    }
    return true;
}

int Filter::compareValue(Value lvalue, Value rvalue, int lIndex, int rIndex){
    if(lvalue.type == TypeVarChar){
        int compLength = (*(int*)lvalue.data>*(int*)rvalue.data)?*(int*)lvalue.data:*(int*)rvalue.data;
        // cout<<"varchar length: "<<compLength<<endl;
        return memcmp((byte*)lvalue.data+4,(byte*)rvalue.data+4,compLength);
    }
    else if(lvalue.type == TypeInt){
        // cout<<*(int*)lvalue.data<<endl;
        if(*(int*)lvalue.data == *(int*)rvalue.data)
            return 0;
        else if(*(int*)lvalue.data < *(int*)rvalue.data)
            return -1;
        else if(*(int*)lvalue.data > *(int*)rvalue.data)
            return 1;
    }
    else if(lvalue.type == TypeReal){
        if(*(float*)lvalue.data == *(float*)rvalue.data)
            return 0;
        else if(*(float*)lvalue.data < *(float*)rvalue.data)
            return -1;
        else if(*(float*)lvalue.data > *(float*)rvalue.data)
            return 1;
    }
}


RC Filter::getNextTuple(void *data){
    //load the next valid tuple into data
    int attrNum = attrs.size();
    int nullNum = ceil((double)attrNum/8);

    // cout << "before getNextTuple" << endl;

    //go through tuples to find valid ones
    while(input->getNextTuple(data) != QE_EOF){
        //utilize the condition onto data
        //null identification
        byte* nullIndicator = (byte*)data;
        byte* nTemp = nullIndicator + (lIndex/8);
        bool nBit = *nTemp & (1<<(7-lIndex%8));
        if(nBit)
            continue;
        if(cond.bRhsIsAttr){
            nTemp = nullIndicator + (rIndex/8);
            nBit = *nTemp & (1<<(7-rIndex%8));
            if(nBit)
                continue;
        }
        //move the offset to find corresponding left value
        int offset = nullNum;
        for(int i = 0; i<lIndex; i++){
            nTemp = nullIndicator + (i/8);
            nBit = *nTemp & (1<<(7-i%8));
            if(nBit)
                continue;
            if(attrs[i].type == TypeVarChar){
                offset += *(int*)((byte*)data + offset) + sizeof(int);
            }
            else
                offset += 4;
        }
        // cout<<"offset is: "<<offset<<endl;


        //if right is a attribute, also find its offset, store the value into rhsValue
        if(cond.bRhsIsAttr){
            int rOffset = nullNum;
            int rLength = 0;
            for(int i = 0; i<rIndex; i++){
                nTemp = nullIndicator + (i/8);
                nBit = *nTemp & (1<<(7-i%8));
                if(nBit)
                    continue;
                if(attrs[i].type == TypeVarChar)
                    rOffset += *(int*)((byte*)data + rOffset) + sizeof(int);
                else
                    rOffset += 4;
            }
            if(attrs[rIndex].type == TypeVarChar){
                rLength = *(int*)((byte*)data + rOffset);
                cond.rhsValue.data = malloc(rLength+sizeof(int));
                memcpy(cond.rhsValue.data,(byte*)data+rOffset,rLength+sizeof(int));
            }
            else{
                cond.rhsValue.data = malloc(4);
                memcpy(cond.rhsValue.data,(byte*)data+rOffset,4);
            }
        }

        //get the corresponding left value and do the compare
        Value lValue;
        lValue.type = attrs[lIndex].type;
        lValue.data = (byte*)data + offset;
        // cout<<"start judge"<<endl;
        // cout<<*(int*)lValue.data<<" compare to "<<*(int*)cond.rhsValue.data<<endl;
        if(condJudge(lValue, cond.rhsValue, lIndex, rIndex))
            return 0;
    }
    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
    this->input->getAttributes(attrs);
}

RC Project::getNextTuple(void *data) {
    vector<Attribute> attrs;
    input->getAttributes(attrs);
    int attrNum = attrs.size();
    int nullNum = ceil((double)attrNum/8);
    void* buffer = malloc(PAGE_SIZE);
    if(input->getNextTuple(buffer) == QE_EOF)
        return QE_EOF;
    byte* nullIndicator = (byte*)buffer;
    //copy the null indicator to data
    memcpy(data, buffer, nullNum);
    //pass the nullindicator
    int offset = nullNum;
    int dataOffset = nullNum;
    //used to count the attrNames
    int nameCtr = 0;
    for(int i = 0; i<attrNum; i++){
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit)  continue;
        int varLength;
        if(attrs[i].type == TypeVarChar)
            varLength = (*(int*)((byte*)buffer+offset)) + 4;
        else
            varLength = 4;
        if(attrs[i].name == attrNames[nameCtr]){
            memcpy((byte*)data+dataOffset,(byte*)buffer+offset,varLength);
            dataOffset += varLength;
            offset += varLength;
            //looking for next name
            nameCtr++;
        }
        else
            offset += varLength;
    }
    free(buffer);
    return 0;
}

RC INLJoin::getNextTuple(void *data){
    vector<Attribute> lattrs;
    leftIn->getAttributes(lattrs);

    //buffer to load record from left 
    void* buffer = malloc(PAGE_SIZE);
    if(leftIn->getNextTuple(buffer) == QE_EOF)
        return QE_EOF;

    //get the corresponding left condition value from buffer
    int nullNum = ceil((double)lattrs.size()/8);
    byte* nullIndicator = (byte*)buffer;
    //move the offset to find corresponding left value
    int offset = nullNum;
    for(int i = 0; i<lIndex; i++){
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit)
            continue;
        if(lattrs[i].type == TypeVarChar)
            offset += *(int*)((byte*)data + offset) + sizeof(int);
        else
            offset += 4;
    }
    byte* lValue = (byte*)buffer + offset;

    // cout<<"offset is: "<<offset<<endl;
    // cout<<"the searching value is "<<*(int*)lValue<<endl;
    //set a iterator for the right
    if(rightIn->setIterator(lValue,lValue,true,true) == -1)
        return -1;


    //get the right value
    void* buffer2 = malloc(PAGE_SIZE);
    //get the right attributes
    vector<Attribute> rAttrs;
    rightIn->getAttributes(rAttrs);
    int rAttrNum = rAttrs.size();
    int rNullNum = ceil((double)rAttrNum/8);

    vector<Attribute> nAttrs;
    this->getAttributes(nAttrs);
    int nAttrNum = nAttrs.size();
    int nNullnum = ceil((double)nAttrNum/8);//temporary value

    // cout<<"before get next tuple"<<endl;

    while(rightIn->getNextTuple(buffer2) != QE_EOF){
        // cout<<"found one"<<endl;
        int innerOffset = nNullnum;
        int lOffset = nullNum;
        int rOffset = rNullNum;
        //form the tuple
        //identify the new nullIndicator
        byte* nNullIndicator = (byte*)data;
        byte* rNullIndicator = (byte*)buffer2;

        for(int i = 0; i<nAttrNum; i++){
            if(i<lattrs.size()){
                //this attribute belongs to the left tuple
                byte* nTemp = nullIndicator + (i/8);
                bool nBit = *nTemp & (1<<(7-i%8));
                if(nBit){
                    //this is a null value
                    byte* newNullptr = nNullIndicator + (i/8);
                    //set it as null
                    *newNullptr = (*newNullptr) | (1<<(7-i%8));
                    continue;
                }
                //it's not null value
                int varLength;
                if(lattrs[i].type == TypeVarChar)
                    varLength = (*(int*)buffer + lOffset) + sizeof(int);
                else
                    varLength = sizeof(int);
                memcpy((byte*)data+innerOffset, (byte*)buffer+lOffset, varLength);
                lOffset += varLength;
                innerOffset += varLength;
             }
            else{
                int rAttrIndex = i - lattrs.size();
                byte* nTemp = rNullIndicator + (rAttrIndex/8);
                bool nBit = *nTemp & (1<<(7-rAttrIndex%8));
                if(nBit){
                    //this is a null value
                    byte* newNullptr = nNullIndicator + (i/8);
                    //set it as null
                    *newNullptr = (*newNullptr) | (1<<(7-i%8));
                    continue;
                }
                //it's not null value
                int varLength;
                if(rAttrs[rAttrIndex].type == TypeVarChar)
                    varLength = (*(int*)buffer2 + rOffset) + sizeof(int);
                else
                    varLength = sizeof(int);
                memcpy((byte*)data+innerOffset, (byte*)buffer2+rOffset, varLength);
                rOffset += varLength;
                innerOffset += varLength;
            }
        }

    }
    free(buffer);
    free(buffer2);
    return 0;
}

 RC Aggregate::getNextTuple(void *data){
    //buffer to load record from left 
    if(resultFlag)
        return QE_EOF;
    void* buffer = malloc(PAGE_SIZE);
    // cout<<"index: "<<aggIndex<<endl;
    void* result = malloc(sizeof(int));
    memset(result,0,4);
    int counter = 0;
    if(op == MIN)
        //init result as a big value
        if(aggAttr.type == TypeInt)
            *(int*)result = 100000;
        else if(aggAttr.type == TypeReal)
            *(float*)result = 100000;
    while(input->getNextTuple(buffer) != QE_EOF){
        //get the corresponding left condition value from buffer
        counter++;
        int nullNum = ceil((double)attrs.size()/8);
        byte* nullIndicator = (byte*)buffer;
        //move the offset to find corresponding left value
        int offset = nullNum;
        for(int i = 0; i<aggIndex; i++){
            byte* nTemp = nullIndicator + (i/8);
            bool nBit = *nTemp & (1<<(7-i%8));
            if(nBit)
                continue;
            if(attrs[i].type == TypeVarChar)
                offset += *(int*)((byte*)buffer + offset) + sizeof(int);
            else
                offset += 4;
        }
        //cout<<offset<<endl;
        byte* lValue = (byte*)buffer + offset;
        //cout<<*(int*)lValue<<endl;
        if(op == MAX){
            if(aggAttr.type == TypeInt)
                if(*(int*)lValue > *(int*)result)
                    memcpy(result, lValue, 4);
            else if(aggAttr.type == TypeReal)
                if(*(float*)lValue > *(float*)result)
                    memcpy(result, lValue, 4);
        }
        else if(op == SUM){
            if(aggAttr.type == TypeInt)
                *(int*)result += *(int*)lValue;
            else if(aggAttr.type == TypeReal)
                *(float*)result += *(float*)lValue;
        }
        else if(op == AVG){
            if(aggAttr.type == TypeInt)
                *(int*)result += *(int*)lValue;
            else if(aggAttr.type == TypeReal)
                *(float*)result += *(float*)lValue;
        }
        else if(op == MIN){
            if(aggAttr.type == TypeInt)
                if(*(int*)lValue < *(int*)result)
                    memcpy(result, lValue, 4);
            else if(aggAttr.type == TypeReal)
                if(*(float*)lValue < *(float*)result)
                    memcpy(result, lValue, 4);
        }
    }
    // if(*(int*)result == 0 || *(int*)result == 100000)
    //     return QE_EOF;
    float output = (float)*(int*)result;
    if(op == AVG)
        output = output/counter;
    if(op == COUNT)
        output = counter;
    memcpy((byte*)data+1,&output,4);
    resultFlag = true;
    return 0;
 }

// ... the rest of your implementations go here


BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        )
{
    this->leftInput = leftIn;
    this->rightInput = rightIn;
    this->numOfPages = numPages;
    this->condition = condition;

    this->leftInput->getAttributes(this->leftTableAttributes);
    this->rightInput->getAttributes(this->rightTableAttributes);
    // cout << "init BNLJ" << endl;

    this->maxBytes = this->numOfPages * PAGE_SIZE;
    this->blockFillBytes = 0;
    this->isFirst = true;
    this->rRecord = malloc(recordMax);
    this->lRecord = malloc(recordMax);
    this->combinedRecord = malloc(2*recordMax);

    // cout <<"here" << endl;

    for(int i = 0; i<leftTableAttributes.size(); i++)
    {

        if(leftTableAttributes[i].name == condition.lhsAttr)
        {
            // cout <<"table l: "<< leftTableAttributes[i].name << "  condition r : " << condition.lhsAttr << endl;
            cout << i << endl;
            lIndex = i;
        }
    }
    for(int i = 0; i < rightTableAttributes.size(); i++)
    {
        if(rightTableAttributes[i].name == condition.rhsAttr)
        {
            // cout <<"table r: "<< rightTableAttributes[i].name << "  condition r : " << condition.rhsAttr << endl;
            cout << i << endl;
            rIndex = i;
        }
    }

}

/***
 * while loadNextBlock() == Fail:
 *     if(firstTime):
 *         while 
 */
RC BNLJoin::getNextTuple(void *data)
{
    // cout << "getNextTuple in BNLJ" << endl;
    if(isFirst)
    {
        loadNextBlock();

    }

    do
    {
        // cout << "begin do " << endl;
        memset(rRecord, 0, recordMax);     
        // if rightInput reach the end of the table  
        if(rightInput->getNextTuple(rRecord) == QE_EOF)
        {
            // cout << "(rightInput->getNextTuple(rRecord) == QE_EOF)" << endl;
            //load next block
            if(loadNextBlock() == QE_EOF)
            {
                // cout << "if(loadNextBlock() == QE_EOF" << endl;
                return QE_EOF;
            }
            else
            {
                rightInput->setIterator();
                rightInput->getNextTuple(rRecord);
            }
        }
        // cout << "here" << endl;

    }while(isValid());

    combineRecords(data);
    isFirst = false;
    return 0;
}

/***
 * Function:
 *      find the key of record of rightTable in the map
 *      if found:
 *          
 * Return: 
 *      If satisfy the condition, return 1;
 *      else, return 0;
 */
void BNLJoin::combineRecords(void* data)
{

    //get the corresponding left condition value from buffer
    int nullNum = ceil((double)leftTableAttributes.size()/8);
    char* nullIndicator = (char*)curFoundRecord;

    //get the right attributes
    int rAttrNum = rightTableAttributes.size();
    int rNullNum = ceil((double)rAttrNum/8);

    vector<Attribute> nAttrs;
    this->getAttributes(nAttrs);
    int nAttrNum = nAttrs.size();
    int nNullnum = ceil((double)nAttrNum/8);//temporary value

    int innerOffset = nNullnum;
    int lOffset = nullNum;
    int rOffset = rNullNum;
    //form the tuple
    //identify the new nullIndicator
    char* nNullIndicator = (char*)data;
    memset(nNullIndicator, 0 ,1);
    char* rNullIndicator = (char*)rRecord;

    for(int i = 0; i<nAttrNum; i++)
    {
        if(i<leftTableAttributes.size()){
            //this attribute belongs to the left tuple
            char* nTemp = nullIndicator + (i/8);
            bool nBit = *nTemp & (1<<(7-i%8));
            if(nBit){
                //this is a null value
                char* newNullptr = nNullIndicator + (i/8);
                //set it as null
                *newNullptr = (*newNullptr) + (1<<(7-i%8));
                continue;
            }
            //it's not null value
            int varLength;
            if(leftTableAttributes[i].type == TypeVarChar)
                varLength = (*(int*)curFoundRecord + lOffset) + sizeof(int);
            else
                varLength = sizeof(int);
            memcpy((char*)data+innerOffset, (char*)curFoundRecord+lOffset, varLength);
            lOffset += varLength;
            innerOffset += varLength;
        }
        else{
            int rAttrIndex = i - leftTableAttributes.size();
            char* nTemp = rNullIndicator + (rAttrIndex/8);
            bool nBit = *nTemp & (1<<(7-rAttrIndex%8));
            if(nBit){
                //this is a null value
                char* newNullptr = nNullIndicator + (i/8);
                //set it as null
                *newNullptr = (*newNullptr) + (1<<(7-i%8));
                continue;
            }
            //it's not null value
            int varLength;
            if(rightTableAttributes[rAttrIndex].type == TypeVarChar)
                varLength = (*(int*)rRecord + rOffset) + sizeof(int);
            else
                varLength = sizeof(int);
            memcpy((char*)data+innerOffset, (char*)rRecord+rOffset, varLength);
            rOffset += varLength;
            innerOffset += varLength;
        }
    }


}

int BNLJoin::isValid()
{
    int attrNum = rightTableAttributes.size();
    // cout << "leftTableAttributes.size()  "  << leftTableAttributes.size()  << endl;
    // cout << "rightTableAttributes.size() " << rightTableAttributes.size() << endl;

    // cout << rightTableAttributes[0].name << endl;
    int nullNum = ceil((double)attrNum/8);
    char* nullIndicator = (char*)this->rRecord;
    char* nTemp = nullIndicator + (rIndex/8);
    bool nBit = *nTemp & (1<<(7-rIndex%8));
    if(nBit)
        return -1;
    //move the offset to find corresponding left value
    int offset = nullNum;
    // cout <<"rIndex: " << rIndex  << endl;

    int i = 0;
    for( ; i<rIndex; i++){
        if(rightTableAttributes[i].type == TypeVarChar){
            offset += *(int*)((char*)this->rRecord + offset) + sizeof(int);
        }
        else
            offset += 4;
    }
    // cout << "is th" << endl;
    // cout << "rightTableAttributes.size(): "<< rightTableAttributes.size() << endl;
    // this->curRAttr = rightTableAttributes[rIndex];
    // cout << "curRAttr: " << this->curRAttr.name << endl;
    this->curRKeyLength = 0;
    if(this->curRAttr.type == TypeVarChar)
    {
        this->curRKeyLength += *(int*)((char*)this->rRecord + offset) + sizeof(int);
    }
    else
        this->curRKeyLength += 4;
    

    // cout << "offset: " << offset << endl;
    void* newKey = malloc(curRKeyLength + 1);
    memset(newKey, 0, curRKeyLength + 1);
    memcpy(newKey, (char*)this->rRecord + offset, curRKeyLength);
    
    if(joinKeyType == TypeInt)
    {
        int keyI = *(int*)newKey;
        map<int, void*>::iterator iter;

        // cout << "cKey: " << cKey << endl;
        
        iter = this->blockMapI.find(keyI);
        if(iter == this->blockMapI.end())
        {
            // cannot find
            free(newKey);
            return -1;
        }
        this->curFoundRecord = iter->second;

    }
    else if(joinKeyType == TypeReal)
    {
        float keyF = *(float*)newKey;
        map<float, void*>::iterator iter;
        
        iter = this->blockMapF.find(keyF);
        if(iter == this->blockMapF.end())
        {
            free(newKey);
            return -1;
        }
        this->curFoundRecord = iter->second;

    }
    else
    {

        int l = *(int*)newKey;
        void* v = malloc(l + 1);
        memcpy(v, (char*)newKey + sizeof(int), l);
        string keyS((char*)v);

        map<string, void*>::iterator iter;
        
        iter = this->blockMapS.find(keyS);
        if(iter == this->blockMapS.end())
        {
            free(newKey);
            return -1;
        }
        // set the curFoundRecord in left 
        this->curFoundRecord = iter->second;
    }
 
    free(newKey);

    return 0;
}

// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
    vector<Attribute> lattrs;
    vector<Attribute> rattrs;
    leftInput->getAttributes(lattrs);
    rightInput->getAttributes(rattrs);
    for(int i = 0; i<lattrs.size(); i++)
        attrs.push_back(lattrs[i]);
    
    for(int i = 0; i<lattrs.size(); i++)
        attrs.push_back(rattrs[i]);
}

// 1. indexScan the leftTable, get the record,   till it fill the block
RC BNLJoin::loadNextBlock()
{
    clearCurBlock();
    // cout << "loadNextBlock" << endl;
    int count = 0;

    int rLength = 0;
    // void* key = malloc(recordMax);
    void* key = NULL;
    blockFillBytes = 0;
    while(blockFillBytes < (maxBytes - recordMax))
    {
        // memset(key, 0, recordMax);
        memset(this->lRecord, 0, recordMax);
        if(leftInput->getNextTuple(this->lRecord))
        {
            // cout << "if(leftInput->getNextTuple(this->lRecord))" << endl;

            return QE_EOF;
        }
        key = getRLengthAndKey(rLength, key);
        addBlockMap(key, rLength);
        blockFillBytes += rLength;
        // cout << "count: " << count++ << endl;

        // cout << "blockFillBytes" << blockFillBytes << endl;
    }
    return 0;
}

void* BNLJoin::showBlockMap()
{
    // map<int, void*>::iterator iter;
    // for(iter = blockMapI.begin(); iter != blockMapI.end(); iter++)
    // {
    //     cout <<"first: " << iter->first << endl;
    //     int a;
    //     memcpy(&a, (char*)iter->second + 5, 4);
    //     cout << "second b: " << a << endl;

    // }
    
}

void* BNLJoin::getRLengthAndKey(int& rLength, void* key)
{
    // cout << "getRLengthAndKey" << endl;
    int attrNum = leftTableAttributes.size();
    int nullNum = ceil((double)attrNum/8);
    char* nullIndicator = (char*)this->lRecord;
    char* nTemp = nullIndicator + (lIndex/8);
    bool nBit = *nTemp & (1<<(7-lIndex%8));
    if(nBit)
        return NULL;
    //move the offset to find corresponding left value
    int offset = nullNum;

    int i = 0;
    for( ; i<lIndex; i++){
        if(leftTableAttributes[i].type == TypeVarChar){
            offset += *(int*)((char*)this->lRecord + offset) + sizeof(int);
        }
        else
            offset += 4;
    }

    this->curAttr = leftTableAttributes[lIndex];
    this->joinKeyType = leftTableAttributes[lIndex].type;
    this->curKeyLength = 0;
    if(this->curAttr.type == TypeVarChar)
    {
            this->curKeyLength += *(int*)((char*)this->lRecord + offset) + sizeof(int);
    }
    else
        this->curKeyLength += 4;
    // cout << "curKeyLength " << curKeyLength <<endl;
    // cout << "offset " << offset << endl;
    key = (char*)this->lRecord + offset;

    for( ; i < leftTableAttributes.size(); i++)
    {
        if(leftTableAttributes[i].type == TypeVarChar){
            offset += *(int*)((char*)this->lRecord + offset) + sizeof(int);
        }
        else
            offset += 4;
    }
    rLength = offset;
    // cout << "cur record Length: " << rLength << endl;
    return key;
}

RC BNLJoin::clearCurBlock()
{   
    if(!isFirst)
    {
        if(joinKeyType == TypeInt)
        {
            map<int, void*>::iterator iter;
            for(iter = blockMapI.begin(); iter != blockMapI.end(); iter++)
            {
                free(iter->second);
            }

            blockMapI.clear();
        }
        else if(joinKeyType == TypeReal)
        {
            map<float, void*>::iterator iter;
            for(iter = blockMapF.begin(); iter != blockMapF.end(); iter++)
            {
                free(iter->second);
            }

            blockMapF.clear();
        }
        else
        {
            map<string, void*>::iterator iter;
            for(iter = blockMapS.begin(); iter != blockMapS.end(); iter++)
            {
                free(iter->second);
            }

            blockMapS.clear();
        }
        
    }
}

RC BNLJoin::addBlockMap(void* key, int rLength)
{

    void* newKey = malloc(curKeyLength + 1);
    memset(newKey, 0, curKeyLength + 1);

    memcpy(newKey, key, curKeyLength);


    // cout << "rLength: " << rLength << endl;
    void* newLRecord = malloc(rLength+1);
    memset(newLRecord, 0, rLength+1);
    memcpy(newLRecord, lRecord, rLength);

    if(joinKeyType == TypeInt)
    {
        int keyI = *(int*)newKey;
        this->blockMapI.insert(pair<int, void*>(keyI, newLRecord)); // dont know whether to change it to vector*
    }
    else if(joinKeyType == TypeReal)
    {
        float keyF = *(int*)newKey;
        this->blockMapF.insert(pair<float, void*>(keyF, newLRecord)); // dont know whether to change it to vector*        
    }
    else
    {
        int l = *(int*)newKey;
        void* v = malloc(l + 1);
        memcpy(v, (char*)newKey + sizeof(int), l);
        string keyS((char*)v);
        this->blockMapS.insert(pair<string, void*>(keyS, newLRecord)); // dont know whether to change it to vector*        
    }

    free(newKey);

}

BNLJoin::~BNLJoin()
{
    // free the malloced variables

    free(this->rRecord);
    free(this->lRecord);
    free(this->combinedRecord);
}