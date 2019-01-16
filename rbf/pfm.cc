#include "pfm.h"
#include <fstream>
#include <iostream>
#include <cstring>
#include <errno.h>
#include <stdio.h>
#include <string.h>

using namespace std;

/*
Hidden Page Format: (unsigned)
| pageNum | read | write | append | rootPageNum |
0		  4      8       12       16            20
*/

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();
    
    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
	numOfHandle = 0;
}


PagedFileManager::~PagedFileManager()
{

}


RC PagedFileManager::createFile(const string &fileName)
{
	FILE* pFile;
	char buffer[PAGE_SIZE];
	char* str = (char*)fileName.data();

	// Only useful for IndexFile
	int rootPageNum = -1;

	pFile=fopen(str,"rb");
	if(pFile==NULL)
	{
		pFile = fopen(str, "wb");
		if(pFile == NULL)
		{
			fclose(pFile);
			return -1;
		}
		else
		{
			memset(buffer, 0, PAGE_SIZE);
			fwrite(&buffer, sizeof(buffer), 1, pFile);

			// set the rootPageNum to -1, Only useful for IndexFile
			fseek(pFile, 4*(sizeof(unsigned)), SEEK_SET);
			fwrite(&rootPageNum, sizeof(int), 1, pFile);

			fclose(pFile);
			return 0;
		}
	}
	else
	{
		fclose(pFile);
		// cout << "file already exist, it can not be created again." << endl;
		return -1;
	}
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	FILE* pFile;

	char* str = (char*)fileName.data(); // transfer string to char*

	if((pFile=fopen(str,"rb"))==NULL) // if file do not exist
	{
		return -1;
	}
	else
	{
		if(remove(str)==0) // if fail to remove file
		{
			fclose(pFile);
			return 0;
		}
		else
		{
			fclose(pFile);
			return -1;
		}
	}

}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	//open the given file
	FILE* pFile;
	char* str = (char*)fileName.data(); // transfer string to char*

	if(!fileHandle.getFileName().empty()) // if the fileHandle already has a file assigned to it.
	{
		return -1;
	}
	

	if((pFile=fopen(str,"rb"))==NULL) // if file do not exist
	{
		return -1;
	}
	else
	{
		if(fileHandle.setFileName(fileName) == -1) return -1;
		fseek(pFile, sizeof(unsigned), SEEK_SET);
		fread(&fileHandle.readPageCounter, sizeof(unsigned), 1, pFile);
		fread(&fileHandle.writePageCounter, sizeof(unsigned), 1, pFile);
		fread(&fileHandle.appendPageCounter, sizeof(unsigned), 1, pFile);
		// fclose(pFile);

		numOfHandle++;
		fclose(pFile);
		return 0;
	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fileHandle.closeFile();
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
	fileName = "";
}

RC FileHandle::setFileName(const string name)
{
	this->fileName = name;
	this->file = fopen(fileName.data(), "rb+");
	if(file == NULL)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}

string FileHandle::getFileName()
{
	return this->fileName;
}

RC FileHandle::closeFile()
{
	// cout << "close file" << endl;

	if(fseek(file, sizeof(unsigned), SEEK_SET)==0) // if seek succeed
	{
		//write the counters into hidden page 
		fwrite(&readPageCounter, sizeof(unsigned), 1, file);
		fwrite(&writePageCounter, sizeof(unsigned), 1, file);
		fwrite(&appendPageCounter, sizeof(unsigned), 1, file);

		if(fclose(file) != 0) perror("Error");
		return 0;
	}
	else
	{
		perror("Error");
		return -1;
	}
	
}

FileHandle::~FileHandle()
{
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
	// cout << "this->fileName " <<this->fileName <<endl;
	if(this->fileName.empty()) // if the handle have not been assigned to a PF
	{
		cout << "no file" << endl;
		return -1;
	}

	if(pageNum >= this->getNumberOfPages())
	{
		cout << "request page num too big" << endl;
		return -1;
	}

	if(fseek(file, (pageNum+1)*PAGE_SIZE, SEEK_SET) == 0)
	{
		fread(data, sizeof(char), PAGE_SIZE, file);
		readPageCounter++;
		return 0;	
	}
	else
	{
		cout << "fail to seek cursor" << endl;
		return -1;
	}
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(this->fileName.empty()) // if the handle have not been assigned to a PF
	{
		return -1;
	}

	if(pageNum > (this->getNumberOfPages()-1)) // if the pageNum exceed the total number of pages
	{
		return -1;
	}

	if(fseek(file, (pageNum+1)*PAGE_SIZE, SEEK_SET)==0) // if seek succeed
	{
		if(fwrite(data, sizeof(char), PAGE_SIZE, file) == PAGE_SIZE)
		{
			writePageCounter++;
			return 0;
		}
	}
	else
	{
		return -1;
	}
}


RC FileHandle::appendPage(const void *data)
{
	unsigned pageNum;
	if(this->fileName.empty()) // if the handle have not been assigned to a PF
	{
		return -1;
	}
		
	pageNum = this->getNumberOfPages();

	if(fseek(file, (pageNum+1) * PAGE_SIZE, SEEK_SET) == 0) // set the cursor to the new page
	{
		fwrite(data, sizeof(char), PAGE_SIZE, file);
		pageNum++; 
		appendPageCounter++;

		//write the Number of Pages into the hidden page
		fseek(file, 0, SEEK_SET); 
		fwrite(&pageNum, sizeof(unsigned), 1, file);
		rewind(file);
		return 0;
	} 
	else
	{
		return -1;
	}
}

unsigned FileHandle::getNumberOfPages() 
{
	unsigned pageNum = -1;

	rewind(file);
	fread(&pageNum, sizeof(unsigned), 1, file);

	// rewind(file);
	// fread(&pageNum, sizeof(unsigned), 1, file);
	return pageNum;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	// cout << "read Countter " << readPageCounter << endl;
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

// write the rootPageNumber to hidden page
RC FileHandle::setRootPageNum(int rootPageNum)
{
	if(fseek(file, 4*(sizeof(unsigned)), SEEK_SET) == 0)
	{
		// cout << "write root pointer position" << ftell(file) << endl;
		fwrite(&rootPageNum, sizeof(int), 1, file);
		return 0;
	}
	else
	{
		return -1;
	}
}

// get the rootPageNumber from hidden page
int FileHandle::getRootpageNum()
{
	int rootPageNum = -1;
	if(fseek(file, 4*(sizeof(unsigned)), SEEK_SET) == 0)
	{
		fread(&rootPageNum, sizeof(int), 1, file);
	}
	return rootPageNum;
}


