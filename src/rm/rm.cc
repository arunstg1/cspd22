#include <iostream>
#include <string.h>
#include <vector>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <algorithm>
#include <set>

#include "rm.h"
#include "../pf/pf.h"

RM* RM::_rm = 0;

using namespace std;

RM* RM::Instance()
{
	if(!_rm)
		_rm = new RM();

	return _rm;
}

RM::RM()
{
}

RM::~RM()
{
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
	std::string fileName = "catalog.metadata";
	int schemaId = 1;
	Catalog catInfo[attrs.size()];
	cout << "attrs.size(): " << sizeof(attrs.size()) << endl;
	cout << "Size of Cat Info: " << sizeof(catInfo) << endl;
	for ( unsigned int i = 0; i < attrs.size(); i++ )
	{
		Attribute attr = attrs.at(i);
		catInfo[i].schemaId = schemaId;

		(catInfo[i].tableName)[0] = '\0';
		strcpy(catInfo[i].tableName, tableName.c_str());

		catInfo[i].columnType = attr.type;
		(catInfo[i].columnName)[0] = '\0';

		strcpy(catInfo[i].columnName, (attr.name).c_str());
		catInfo[i].columnLength = attr.length;
	}
	if( createDatTable(tableName) == 0) {
		ofstream output_file(fileName.c_str(), ios::app | ios::binary);
		output_file.write((char*)&catInfo, sizeof(Catalog)*(attrs.size()));
		output_file.close();
		return 0;
	} else {
		cout<< "ERROR: Create table failed while creating the data file for table : " << tableName;
	}
	return -1;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
	int maxVersion = 0;
	string fileName = "catalog.metadata";
	vector<Catalog *> catVector;
	ifstream input_file(fileName.c_str(), ios::binary);
	Catalog *master;

	while(input_file.good())
	{
		master = new Catalog;
		memset(master, 0, sizeof(Catalog));
		input_file.read((char*)master, sizeof(Catalog));
		if( (strcmp(master->tableName, tableName.c_str()) == 0) && (master->schemaId != -1) )
		{
			if( master->schemaId >= maxVersion )
			{
				maxVersion = master->schemaId;
			}
			catVector.push_back(master);
		}
	}

	for(unsigned i=0; i< catVector.size(); i++)
	{
		Catalog* cat = new Catalog;
		memset(cat, 0, sizeof(Catalog));
		cat = catVector.at(i);
		if( (cat->schemaId == maxVersion) && strcmp(master->tableName, tableName.c_str()) )
		{
			Attribute attr;
			attr.name = cat->columnName;
			attr.type = cat->columnType;
			attr.length = cat->columnLength;
			attrs.push_back(attr);
		}
	}
	return 0;
}

RC RM::createDatTable(const string tableName)
{
	PF_Manager *pf = PF_Manager::Instance();
	string fileName = tableName+ ".dat";
	if( pf->CreateFile(fileName.c_str()) == 0 ) {
		if( initDatTable(fileName.c_str(), pf) == 0 ) {
			return 0;
		} else {
			pf->DestroyFile(tableName.c_str());
			cout << "ERROR: Creation of dat file for the table " << tableName << " failed.";
		}
	}
	return -1;
}

RC RM::initDatTable(const string tableName, PF_Manager *pf)
{
	void *buffer = malloc(PF_PAGE_SIZE);
	short diLen = PF_PAGE_SIZE;

	memcpy((char *)buffer, &diLen, sizeof(short));

	PF_FileHandle fileHandle;
	if( pf->OpenFile(tableName.c_str(), fileHandle) == 0 )
	{
		if( fileHandle.WritePage(0, buffer) == 0 )
		{
			pf->CloseFile(fileHandle);
			free(buffer);
			return 0;
		}
	}
	free(buffer);
	return -1;
}

RC RM::deleteTuples(const string tableName)
{
	PF_Manager *pf = PF_Manager::Instance();
	string filename = tableName + ".dat";
	if( pf->DestroyFile(filename.c_str()) == 0 )
	{
		if(createDatTable(tableName) == 0)
			return 0;
		else {
			cout << "ERROR: Delete tuples operation failed to create a fresh file for table " << tableName;
			return -1;
		}
	}
	cout << "ERROR: Delete tuples operation failed to destroy file for table " << tableName;
	return -1;
}

RC RM::deleteTable(const string tableName)
{
	string fileName = "catalog.metadata";
	vector<Catalog> catVector;
	ifstream input_file(fileName.c_str(), ios::binary);
	Catalog master;

	while(input_file.good())
	{
		input_file.read((char*)&master, sizeof(Catalog));
		catVector.push_back(master);
	}

	Catalog modifyCat[catVector.size()];
	for( unsigned i = 0; i < catVector.size(); i++ )
	{
		Catalog tempCat = catVector.at(i);
		string tempStr = string(tempCat.tableName);

		if( strcmp(tempStr.c_str(), tableName.c_str()) == 0 )
			tempCat.schemaId = -1;

		modifyCat[i] = tempCat;
	}
	ofstream output_file(fileName.c_str(), ios::binary|ios::ate);
	output_file.write((char*)&modifyCat, sizeof(Catalog)*(catVector.size()));
	output_file.close();

	PF_Manager *pf = PF_Manager::Instance();
	string dataFile = tableName + ".dat";
	pf->DestroyFile(dataFile.c_str());

	return 0;
}

short RM::getDIOffset(void *page)
{
	short DIOffset = -1;
	memcpy(&DIOffset, (char *)page, sizeof(short));
	if(DIOffset <= 0 || DIOffset > PF_PAGE_SIZE) {
		cout << "ERROR : Invalid DIOffset read from file" << endl;
		exit(-1);
	}
	return DIOffset;
}

RC RM::setDIOffset(void *page, short DIOffset)
{
	if(DIOffset < (short)sizeof(short) || DIOffset > (short)PF_PAGE_SIZE) {
		cout << "ERROR: Invalid DIOffset attempted to be written to page." << endl;
		return(-1);
	}
	memcpy((char *)page, &DIOffset, sizeof(short));
	return 0;
}

RC RM::setDIRecord(void *page, int offset, int startPos, short length, short version)
{
	DIRecord di;
	di.startPos = startPos;
	di.length = length;
	di.version = version;
	memcpy((char *)page + offset, &di, sizeof(DIRecord));
	return 1;
}

RC RM::getLatestVersion(string tableName)
{
	return 1;
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	if (pf->OpenFile(fileName.c_str(), *DBHandle) != 0){
		cout << "ERROR: Insert operation is unable to open table: " << tableName;
		return -1;
	}

	void *currentPage = malloc(PF_PAGE_SIZE);
	bool isInserted = false;
	int currentPageNumber = 0;
	int recordLength = calcDataLength(tableName, data);

	while(! isInserted) {

		if((DBHandle->ReadPage(currentPageNumber, currentPage)) != 0) {   // this will happen when we have read all the pages
			cout << "New page created.";
			currentPage = malloc(PF_PAGE_SIZE);
			setDIOffset(currentPage, PF_PAGE_SIZE);
		}

		vector<CITuple> CI;
		generateCI(currentPage, CI);

		CITuple lastCI = CI.at(CI.size() -1);
		int maxSpaceInPage = lastCI.end - lastCI.beg + 1;

		if(recordLength > maxSpaceInPage ||
				(((lastCI.end + 1) == getDIOffset(currentPage)) && (maxSpaceInPage < (recordLength +sizeof(DIRecord))) ))
		{
			currentPageNumber++;        // goto next page in the data file
			continue;
		}
		else
		{
			for(int i = 0; i < (int)CI.size(); i++) {
				short space = CI.at(i).end - CI.at(i).beg + 1;
				if(space >= recordLength) {
					memcpy((char *)currentPage + CI.at(i).beg, data, recordLength);
					short newDIOffset = getDIOffset(currentPage) - sizeof(DIRecord);
					setDIRecord(currentPage, newDIOffset, CI.at(i).beg, recordLength, getLatestVersion(tableName));
					setDIOffset(currentPage, newDIOffset);
					if(DBHandle->WritePage(currentPageNumber, currentPage) !=0) {
						cout << "ERROR: Insert unable to write data to table " << tableName;
						pf->CloseFile(*DBHandle);
						return -1;
					}
					isInserted = true;

					rid.pageNum = currentPageNumber;
					rid.slotNum = newDIOffset;
					break;
				}
			}
		}
	}
	free(currentPage);
	pf->CloseFile(*DBHandle);
	return 0;
}

RC RM::calcDataLength(const string tableName, const void *data)
{
	unsigned totLength = 0;
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	for(int i=0; i< (int)attrs.size(); i++)
	{
		if( attrs[i].type == 0 )
		{
			totLength += sizeof(int);
		}
		else if( attrs[i].type == 1 )
		{
			totLength += sizeof(float);
		}
		else if( attrs[i].type == 2 )
		{
			int tempLen = 0;
			memcpy(&tempLen, (char *)data + totLength, sizeof(int));
			totLength += (tempLen + sizeof(int));
		}
	}
	return totLength;
}

RC RM::deleteTuple(const string tableName, const RID &rid)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle DBHandle;

	cout << "Del Tuple : rid page num = " << rid.pageNum << ", slot num = " << rid.slotNum << endl;
	if(pf->OpenFile(fileName.c_str(), DBHandle) == 0) {
		void * buffer = malloc(PF_PAGE_SIZE);

		if(DBHandle.ReadPage(rid.pageNum, buffer) == 0) {
			DIRecord diRec;
			memcpy(&diRec, (char *)buffer + rid.slotNum, sizeof(DIRecord));
			cout << "Del Tuple : start pos = " << diRec.startPos << ", len = " << diRec.length << endl;

			if (diRec.startPos <= 0) {    // the record actually lies in same different page
				void *targetPage = malloc(PF_PAGE_SIZE);

				if(DBHandle.ReadPage((-1*diRec.startPos)-PF_PAGE_SIZE, targetPage) == 0) {
					DIRecord targetDIRec;
					memcpy(&targetDIRec, (char *)targetPage + diRec.length, sizeof(DIRecord));
					cout << "Del Tuple : overflow rec start pos = " << targetDIRec.startPos << ", len = " << targetDIRec.length << endl;
					setDIRecord(targetPage,diRec.length, PF_PAGE_SIZE+1, targetDIRec.length, 0);
					DBHandle.WritePage(((-1*diRec.startPos)-PF_PAGE_SIZE), targetPage);
				}
			}
			setDIRecord(buffer, rid.slotNum, PF_PAGE_SIZE+1, diRec.length, 0);
			DBHandle.WritePage(rid.pageNum, buffer);
		} else {
			cout << "ERROR: delete tuple operation was not able to read the page #" << rid.pageNum << " of table : " << tableName;
			return -1;
		}
		pf->CloseFile(DBHandle);
	} else {
		cout << "ERROR: delete tuple operation was not able to open the data file for table : " << tableName;
		return -1;
	}
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();

	if(pf->OpenFile(fileName.c_str(), *DBHandle) == 0){
		void * buffer = malloc(PF_PAGE_SIZE);

		if(DBHandle->ReadPage(rid.pageNum, buffer) == 0){
			DIRecord diRec;
			memcpy(&diRec, (char *)buffer + rid.slotNum, sizeof(DIRecord));
			int dioffset = getDIOffset(buffer);
			cout << "start pos = " << diRec.startPos << ", len = " << diRec.length << endl;
			if(rid.slotNum < dioffset) {
				pf->CloseFile(*DBHandle);
				return -1;
			}
			if(diRec.startPos >= (int) sizeof(short) && diRec.startPos < PF_PAGE_SIZE)  // the read is in the same page
			{
				memcpy(data, (char *)buffer + diRec.startPos, diRec.length);
				pf->CloseFile(*DBHandle);
				return 0;
			}
			else if(diRec.startPos < 0) {    // the record actually lies in same different page
				if(DBHandle->ReadPage((-1*diRec.startPos)-PF_PAGE_SIZE, buffer) == 0) {
					memcpy(&diRec, (char *)buffer + diRec.length, sizeof(DIRecord));
					cout << "OVERFLOW rec start pos = " << diRec.startPos << ", len = " << diRec.length << endl;
					memcpy(data, (char *)buffer + diRec.startPos, diRec.length);
					pf->CloseFile(*DBHandle);
					return 0;
				}
			} else {
				pf->CloseFile(*DBHandle);
				return -1;
			}
			pf->CloseFile(*DBHandle);
			return -1;
		}
	}
	pf->CloseFile(*DBHandle);
	return -1;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	int offset = 0, readLength = 0;
	void *data_returned = malloc(100);
	readTuple(tableName, rid, data_returned);
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);

	for(int i=0; i< (int)attrs.size(); i++) {
		if( attrs[i].type == (int)0) {
			if(attrs[i].name == attributeName) {
				readLength = sizeof(int);
				break;
			} else {
				offset += sizeof(int);
			}
		} else if( attrs[i].type == (int)1) {
			if(attrs[i].name == attributeName) {
				readLength = sizeof(float);
				break;
			} else {
				offset += sizeof(float);
			}
		} else if( attrs[i].type == (int)2) {
			if(attrs[i].name == attributeName) {
				memcpy(&readLength, (char *)data_returned + offset, sizeof(int));
				offset += sizeof(int);
				break;
			} else {
				int tempLen = 0;
				memcpy(&tempLen, (char *)data_returned + offset, sizeof(int));
				offset += (tempLen + sizeof(int));
			}
		}
	}
	memcpy(data, (char *)data_returned + offset, readLength);
	return 0;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	if(pf->OpenFile(fileName.c_str(), *DBHandle) != 0){
		cout << "ERROR: Reorganize page is unable to open table: " << tableName;
		return -1;
	}

	void *buffer = malloc(PF_PAGE_SIZE);
	void *reorganisedBuffer = malloc(PF_PAGE_SIZE);
	if(DBHandle->ReadPage(pageNumber, buffer) != 0) {
		cout << "ERROR: Reorganise page is unable to read data from table: " << tableName;
		return -1;
	}

	int DIOffset = getDIOffset(buffer);
	DIRecord diRec, modifiedDiRec;
	int nextDataPos = sizeof(short); 				// by default data will be written after the offset

	setDIOffset(reorganisedBuffer, DIOffset);	// copy the DI offset as it will remain the same

	for(int i=PF_PAGE_SIZE-sizeof(DIRecord); i >= DIOffset; i -= sizeof(DIRecord))
	{
		memcpy(&diRec, (char *)buffer + i, sizeof(DIRecord));

		modifiedDiRec.startPos = diRec.startPos;					// keep the DI rec as is
		modifiedDiRec.length = diRec.length;
		modifiedDiRec.version = diRec.version;

		if(diRec.startPos < PF_PAGE_SIZE && diRec.startPos >= (int)sizeof(short)) {	// check if the DIrec corresponds to a
			// record that is existing in the same page
			memcpy((char *)reorganisedBuffer + nextDataPos, (char *)buffer + diRec.startPos, diRec.length);
			modifiedDiRec.startPos = nextDataPos;
			nextDataPos = nextDataPos + diRec.length;
		} else if(diRec.startPos > PF_PAGE_SIZE){ // for deleted records, make the length as 0 as
			modifiedDiRec.length = 0;			  // that space might have been used while re-organization
			modifiedDiRec.version = 0;
		} else if (diRec.startPos <= 0)	{		  // for records shifted to another page, no need to do anything
		} else { 								  // this cannot happen
			cout << "ERROR: Re-organise page came to an undefined condition !!";
			return -1;
		}
		memcpy((char *)reorganisedBuffer + i, &modifiedDiRec, sizeof(DIRecord));
	}

	if(DBHandle->WritePage(pageNumber, reorganisedBuffer) != 0){
		cout << "ERROR: Reorganise page is unable to write data to the table: " << tableName;
		return -1;
	}

	free(buffer);
	free(reorganisedBuffer);
	return 0;
}

RC RM::generateCI(void *data, vector<CITuple> &CI)
{
	int diOffset = getDIOffset(data);
	DIRecord diRec;

	CITuple cit;
	cit.beg = sizeof(short); cit.end = diOffset-1;
	CI.push_back(cit);

	for(short i = PF_PAGE_SIZE - sizeof(DIRecord); i >= diOffset; i -= sizeof(DIRecord))
	{
		memcpy(&diRec, (char *)data + i, sizeof(DIRecord));
		if(diRec.startPos >= (int)sizeof(short) && ((diRec.startPos+diRec.length) < PF_PAGE_SIZE)) { // for each record on this page

			for(int j = 0; j < (int)CI.size(); j++) {
				int recordStartPos = diRec.startPos;
				int recordEndPos = diRec.startPos + diRec.length;
				CITuple currentCI = CI.at(j);

				if(recordStartPos > currentCI.beg && recordEndPos < currentCI.end) {
					CITuple newCI;
					newCI.beg = recordEndPos;
					newCI.end = currentCI.end;
					CI.push_back(newCI);
					currentCI.end = recordStartPos - 1;
					CI[j] = currentCI;
				} else if (recordStartPos == currentCI.beg) {
					currentCI.beg = recordEndPos;
					CI[j] = currentCI;
				} else if (recordEndPos == currentCI.end) {
					currentCI.end = recordStartPos - 1;
					CI[j] = currentCI;
				}
			}
		}
	}
	sort(CI.begin(), CI.end());
	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	pf->OpenFile(fileName.c_str(), *DBHandle);

	void * buffer = malloc(PF_PAGE_SIZE);
	DBHandle->ReadPage(rid.pageNum, buffer);

	DIRecord diRec, targetDiRec;
	int offset = rid.slotNum;
	memcpy(&diRec, (char *)buffer + offset, sizeof(DIRecord));

	int originalRecLen;
	bool overflow = false;
	void * targetPage;

	if(diRec.startPos >= (int)sizeof(short))
		originalRecLen = diRec.length;
	else {            // this is a overflow record !!
		overflow = true;
		targetPage = malloc(PF_PAGE_SIZE);

		if(DBHandle->ReadPage((-1*diRec.startPos)-PF_PAGE_SIZE, targetPage) == 0) {
			memcpy(&targetDiRec, (char *)targetPage + diRec.length, sizeof(DIRecord));
			cout << "UPDATE an overflow rec : start pos = " << diRec.startPos << ", len = " << diRec.length << endl;
			cout << "UPDATE an overflow rec : Rec physically is at page #"<< (-1*diRec.startPos)-PF_PAGE_SIZE;
			cout << " start pos = " << targetDiRec.startPos << ", len = " << targetDiRec.length << endl;

			originalRecLen = targetDiRec.length;
		} else {
			cout << "ERROR: Update failed to read page " << (-1*diRec.startPos)-PF_PAGE_SIZE << " while reading overflow record"<< endl;
			return -1;
		}
	}

	int modifiedRecLen = calcDataLength(tableName, data);
	void *page = malloc(PF_PAGE_SIZE);
	unsigned int currentPageNum = rid.pageNum;

	bool isRecordUpdated = false;
	while(!isRecordUpdated)
	{
		if((DBHandle->ReadPage(currentPageNum, page)) != 0) {     // this will happen when we have read all the pages
			cout << "UPDATE: new page created.";
			page = malloc(PF_PAGE_SIZE);                        // and none had sufficent space
			setDIOffset(page, PF_PAGE_SIZE);                        // we end up creating a new page
			currentPageNum = DBHandle->GetNumberOfPages();
		}

		if(rid.pageNum == currentPageNum && overflow == false) // hide
			setDIRecord(page, rid.slotNum, PF_PAGE_SIZE+1, diRec.length, diRec.version);

		vector<CITuple> CI;
		generateCI(page, CI);

		if(rid.pageNum == currentPageNum && overflow == false) // unhide
			setDIRecord(page, rid.slotNum, diRec.startPos, diRec.length, diRec.version);

		CITuple lastCI = CI.at(CI.size() -1);
		int maxSpaceInPage = lastCI.end - lastCI.beg + 1;

		if(modifiedRecLen > maxSpaceInPage ||
				(((lastCI.end + 1) == getDIOffset(page)) && (maxSpaceInPage < (modifiedRecLen+sizeof(DIRecord))) ))
		{
			// goto next page in the data file
			if(rid.pageNum == 0 && rid.pageNum == currentPageNum)
				currentPageNum = 1;
			else if(rid.pageNum == currentPageNum)
				currentPageNum = 0;
			else {
				currentPageNum++;
				if(rid.pageNum == currentPageNum) currentPageNum++;
			}
			continue;
		}
		else			// find the optimal location for the new record and insert it
		{
			isRecordUpdated = true;

			for(int i = 0; i < (int) CI.size(); i++) {
				int space = CI.at(i).end - CI.at(i).beg + 1;
				if(space >= modifiedRecLen) {
					memcpy((char *)page + CI.at(i).beg, data, modifiedRecLen);

					if(overflow == true){
						setDIRecord(targetPage, diRec.length, PF_PAGE_SIZE+targetDiRec.startPos, targetDiRec.length, 0);
						int targetDIOffset = getDIOffset(targetPage);
						if(diRec.length == targetDIOffset) {
							targetDIOffset += sizeof(DIRecord);
							setDIOffset(targetPage, targetDIOffset);
						}
						DBHandle->WritePage((-1*diRec.startPos)-PF_PAGE_SIZE, targetPage);
					}

					int latestVersion = getLatestVersion(tableName);
					if(currentPageNum == rid.pageNum) { // if its the same page
						setDIRecord(page, rid.slotNum, CI.at(i).beg, modifiedRecLen, latestVersion);
					} else {
						int targetPageDIRecOffset = getDIOffset(page) - sizeof(DIRecord);
						setDIRecord(page, targetPageDIRecOffset, CI.at(i).beg, modifiedRecLen, latestVersion);
						setDIOffset(page, targetPageDIRecOffset);

						setDIRecord(buffer, rid.slotNum, (-1 * (PF_PAGE_SIZE+currentPageNum)), targetPageDIRecOffset, latestVersion);
						DBHandle->WritePage(rid.pageNum, buffer);
					}
					DBHandle->WritePage(currentPageNum, page);
					pf->CloseFile(*DBHandle);

					cout << "Updated for rec in page #" << rid.pageNum << " slot num = " << rid.slotNum << endl;
					cout << "Updated rec in page #" << currentPageNum << " start=" << CI.at(i).beg << " end=" << (CI.at(i).beg + modifiedRecLen) <<endl;
					break;
				}
			}
		}
	}
	free(page);
	free(buffer);
	return 0;
}

RC RM::reorganizeTable(const string tableName)
{
	string fileName = tableName + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();

	if(pf->OpenFile(fileName.c_str(), *DBHandle) != 0){
		cout << "ERROR: Reorganise table is unable to open the table: " << tableName;
		return -1;
	}

	int newPageIndex = 0, oldPageIndex = 0;
	void *newPage = malloc(PF_PAGE_SIZE);
	void *oldPage = malloc(PF_PAGE_SIZE);
	int totalPages = DBHandle->GetNumberOfPages();
	int oldDIOffset,
	newDIOffset = PF_PAGE_SIZE,
	newNextStartPos = sizeof(short);

	while(oldPageIndex < totalPages)
	{
		if(DBHandle->ReadPage(oldPageIndex, oldPage) != 0) {
			cout << "ERROR: Reorganise table is not able to read the page #" << oldPageIndex << " for table " << tableName;
			pf->CloseFile(*DBHandle);
			return -1;
		}

		oldDIOffset = getDIOffset(oldPage);
		DIRecord diRec;

		for(int i = PF_PAGE_SIZE - sizeof(DIRecord); i >= oldDIOffset; i -= sizeof(DIRecord))
		{
			memcpy(&diRec, (char *)oldPage + i, sizeof(DIRecord));

			if(diRec.startPos >= (int)sizeof(short) && diRec.startPos < PF_PAGE_SIZE) {
				int availableSpace = (newDIOffset - newNextStartPos);
				int requetstedSpace = (diRec.length + sizeof(DIRecord));

				if(requetstedSpace > availableSpace) {
					setDIOffset(newPage, newDIOffset);
					if(DBHandle->WritePage(newPageIndex, newPage) != 0) {
						cout << "ERROR: Re-organise table could not write to table " << tableName;
						return -1;
					}

					newPageIndex++;
					newNextStartPos = sizeof(short);
					newDIOffset = PF_PAGE_SIZE;
					memset(newPage, 0, PF_PAGE_SIZE);
				}

				newDIOffset -= sizeof(DIRecord);
				setDIRecord(newPage, newDIOffset, newNextStartPos, diRec.length, diRec.version);
				memcpy((char *)newPage + newNextStartPos, (char *) oldPage + diRec.startPos, diRec.length);
				newNextStartPos += diRec.length;
			}
		}
		oldPageIndex++;
	}
	// write back the last page
	setDIOffset(newPage, newDIOffset);

	if(newNextStartPos != 2 && DBHandle->WritePage(newPageIndex, newPage) != 0) {
		cout << "ERROR: Re-organise table could not write to table " << tableName;
		return -1;
	}

	free(newPage);
	free(oldPage);

	if(pf->CloseFile(*DBHandle) != 0) {
		cout << "ERROR: Re-organise table could not close the table " << tableName;
		return -1;
	}

	if(truncate(fileName.c_str(), ((newPageIndex+1)*PF_PAGE_SIZE)) != 0) {
		// delete all the pages from
		// (newPageindex+1) till (total pages-1)
		cout << "ERROR: Re-organise table could not truncate the file for table " << tableName;
		return -1;
	}

	return 0;
}

RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RM_ScanIterator &rmsi)
{
	rmsi.setTableName(tableName);
	rmsi.setConditionAttribute(conditionAttribute);
	rmsi.setCompOp(compOp);
	rmsi.setValue(value);
	rmsi.setAttributeNames(attributeNames);

	void *buffer = malloc(PF_PAGE_SIZE);
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	string fileName = tableName + ".dat";
	pf->OpenFile(fileName.c_str(), *DBHandle);
	DBHandle->ReadPage(0,buffer);
	rmsi.setPageBuffer(buffer);

	int diOffset = this->getDIOffset(buffer);
	rmsi.setDiOffset(diOffset);
	if( diOffset < PF_PAGE_SIZE ) {
		rmsi.setCurrentDiPos(PF_PAGE_SIZE - sizeof(DIRecord));
		return 0;
	}
	return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	bool foundTuple = false;
	int diOffset = this->getDiOffset();
	int currentDI = this->getCurrentDiPos();
	int pageNumber;
	RM *rm = RM::Instance();
	void* buffer = malloc(PF_PAGE_SIZE);
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	string fileName = tableName + ".dat";
	bool dontTouch = false;
	while(!foundTuple) {
		if(currentDI < diOffset) { // All DIs of the current page is processed. Need to move to the next page.
			pf->OpenFile(fileName.c_str(), *DBHandle);
			pageNumber = this->getPage();
			this->setPage(pageNumber+1);
			if( DBHandle->ReadPage(pageNumber+1,buffer) != 0 ) {
				return -1;
			}
			dontTouch = true;
			this->setPageBuffer(buffer);
			int diOffset = rm->getDIOffset(buffer);
			this->setDiOffset(diOffset);
			if( diOffset != PF_PAGE_SIZE ) {
				this->setCurrentDiPos(PF_PAGE_SIZE - sizeof(DIRecord));
				currentDI = PF_PAGE_SIZE - sizeof(DIRecord);
			}
		}
		if(currentDI == PF_PAGE_SIZE || currentDI == 0) {
			return -1;
		}
		string tableName = this->getTableName();
		string conditionAttribute = this->getConditionAttribute();
		CompOp compOp = this->getCompOp();
		const void* value = this->getValue();
		vector<string> attributeNames = this->getAttributeNames();


		if(!dontTouch) {
			const void *buffer1 = malloc(PF_PAGE_SIZE);
			buffer1 = this->getPageBuffer();
			buffer = const_cast<void*>(buffer1);
		}

		bool addTuple = true;

		vector<pair<string,int> > catDetails;
		vector<Attribute> catAttr;
		rm->getAttributes(tableName, catAttr);

		for(unsigned i = 0; i< catAttr.size(); i++) { // Reading the details of the catalog into the map
			int type = catAttr[i].type;
			string name = catAttr[i].name;
			catDetails.push_back(std::make_pair(name, type));
		}

		pageNumber = this->getPage();

		addTuple = true;
		void* reqData = malloc(250);
		int reqDataOffset = 0;
		DIRecord diRec;
		memcpy(&diRec, (char *)buffer + currentDI, sizeof(DIRecord));

		while( diRec.startPos > PF_PAGE_SIZE || diRec.startPos < 0 ) { // For Deleted Records, Skip it and pick the next DI entry.
			currentDI -= sizeof(DIRecord);
			memcpy(&diRec, (char *)buffer + currentDI, sizeof(DIRecord));
		}

		// Start from here
		memcpy(data, (char *)buffer + diRec.startPos, diRec.length);
		int currDataLen = rm->calcDataLength(tableName, data);
		RC tempLen = 0, counter = 0;

		bool conditionExists = false, isConditionPresent = false;
		addTuple = true;

		while( (tempLen <= currDataLen) && (counter < (int)catDetails.size()) ) { // Iterating thru the current tuple
			isConditionPresent = false, conditionExists = false;
			string clmName = catDetails.at(counter).first;
			int clmType =  catDetails.at(counter).second;
			bool isPresent = false;

			for( int i=0; i< (int)attributeNames.size(); i++ ) {
				if( attributeNames.at(i) == clmName ) { // Checking if the current attribute should be added to the resultant data
					isPresent = true;
				}
				if( compOp != NO_OP && !conditionExists ) { // Checking if the condition exists on the current attribute
					if( conditionAttribute == clmName ) {
						conditionExists = true;
					}
				}
			}

			if( clmType == 0 ) {
				int tempInt;
				memcpy(&tempInt, (char *)data + tempLen, sizeof(int));

				if( conditionExists ) {
					if( compOp == EQ_OP ) {
						if( tempInt == *(int*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == LT_OP ) {
						if( tempInt < *(int*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == GT_OP ) {
						if( tempInt > *(int*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == LE_OP ) {
						if( tempInt <= *(int*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == GE_OP ) {
						if( tempInt >= *(int*) value );
						else {
							addTuple = false;
						}
					}  else if( compOp == NE_OP ) {
						if( tempInt != *(int*) value );
						else {
							addTuple = false;
						}
					}
				}

				if( isPresent && addTuple ) {
					memcpy((char *)reqData + reqDataOffset, &tempInt, sizeof(int));
					reqDataOffset += sizeof(int);
					//cout << tempInt << endl;
				}
				tempLen += sizeof(int);
			} else if( clmType == 1 ) {
				float tempFloat = 0.00;
				memcpy(&tempFloat, (char *)data + tempLen, sizeof(float));

				if( conditionExists ) {
					if( compOp == EQ_OP ) {
						if( tempFloat == *(float*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == LT_OP ) {
						if( tempFloat < *(float*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == GT_OP ) {
						if( tempFloat > *(float*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == LE_OP ) {
						if( tempFloat <= *(float*) value );
						else {
							addTuple = false;
						}
					} else if( compOp == GE_OP ) {
						if( tempFloat >= *(float*) value );
						else {
							addTuple = false;
						}
					}  else if( compOp == NE_OP ) {
						if( tempFloat != *(float*) value );
						else {
							addTuple = false;
						}
					}
				}

				if( isPresent && addTuple ) {
					memcpy((char *)reqData + reqDataOffset, &tempFloat, sizeof(float));
					reqDataOffset += sizeof(float);
					//cout << tempFloat << endl;
				}
				tempLen += sizeof(float);
			} else if( clmType == 2 ) {
				char* tempStr = (char *)malloc(100);
				int tempInt = 0;
				memcpy(&tempInt, (char *)data + tempLen, sizeof(int));
				memcpy(tempStr, (char *)data + tempLen + sizeof(int), tempInt);
				tempStr[tempInt] = '\0';

				if( conditionExists ) {
					if( compOp == EQ_OP ) {
						if( strcmp(tempStr, (char*) value) == 0 );
						else {
							addTuple = false;
						}
					} else if( compOp == LT_OP ) {
						if( strcmp(tempStr, (char*) value) < 0 );
						else {
							addTuple = false;
						}
					} else if( compOp == GT_OP ) {
						if( strcmp(tempStr, (char*) value) > 0 );
						else {
							addTuple = false;
						}
					} else if( compOp == LE_OP ) {
						if( strcmp(tempStr, (char*) value) <= 0 );
						else {
							addTuple = false;
						}
					} else if( compOp == GE_OP ) {
						if( strcmp(tempStr, (char*) value) >= 0 );
						else {
							addTuple = false;
						}
					}  else if( compOp == NE_OP ) {
						if( strcmp(tempStr, (char*) value) != 0 );
						else {
							addTuple = false;
						}
					}
				}

				if( isPresent && addTuple ) {
					memcpy((char *)reqData + reqDataOffset, &tempInt, sizeof(int));
					memcpy((char *)reqData + reqDataOffset + sizeof(int), tempStr, tempInt);
					//cout << (tempStr) << endl;
					reqDataOffset += (tempInt + sizeof(int));
				}
				tempLen += (tempInt + sizeof(int));
				free(tempStr);
			}
			counter++;
		}
		if( addTuple ) {
			memcpy(data, reqData, reqDataOffset);
			rid.pageNum = pageNumber; rid.slotNum = diRec.startPos;
			foundTuple = true;
			currentDI -= sizeof(DIRecord);
			this->setCurrentDiPos(currentDI);
			return 0;
		}
		free(reqData);
		currentDI -= sizeof(DIRecord);
		this->setCurrentDiPos(currentDI);
	}
	return -1;
}

void* RM::parseTuple(void *data, const string tableName, int &tupleLength)
{
	void * tempData = malloc(100);
	short tempLength = 0;
	int tempDataOffset = 0;
	vector<Attribute> attrs;
	int tupleLen = this->calcDataLength(tableName, data);
	getAttributes(tableName, attrs);

	int stringCnt = this->stringInTuple(attrs);
	tupleLength = tupleLen + stringCnt*sizeof(short);
	tempData = malloc(tupleLength);

	for( int i=0; i< (int)attrs.size(); i++ ) {
		Attribute attr = attrs.at(i);
		if( attr.type == 0 ) {
			tempLength += sizeof(int);
		} else if( attr.type == 1 ) {
			tempLength += sizeof(int);
		} else if( attr.type == 2 ) {
			int stringLength = 0;
			memcpy(&stringLength, (char*) data + tempLength, sizeof(int));
			tempLength += (sizeof(int) + stringLength);
			memcpy((char*) tempData + tempDataOffset, &stringLength, sizeof(short));
			tempDataOffset += sizeof(short);
		}
	}

	memcpy((char*) tempData + tempDataOffset, data, tupleLen);

	return tempData;
}


void* RM::unParseTuple(void *data, const string tableName)
{
	void * tempData = malloc(100);
	vector<Attribute> attrs;
	getAttributes(tableName, attrs);
	int stringCnt = this->stringInTuple(attrs); int stringLength = stringCnt*sizeof(short);
	int tempLen = stringLength;

	for( int i=0; i< (int)attrs.size(); i++ ) {
		Attribute attr = attrs.at(i);
		if( attr.type == 0 ) {
			int tempInt;
			// Getting value from data
			memcpy(&tempInt, (char *)data + tempLen, sizeof(int));

			// Storing the obtained value in the tempData
			memcpy((char *)tempData + tempLen - stringLength, &tempInt , sizeof(int));
			tempLen += sizeof(int);
		} else if( attr.type == 1 ) {
			float tempFloat;
			// Getting value from data
			memcpy(&tempFloat, (char *)data + tempLen, sizeof(float));

			// Storing the obtained value in the tempData
			memcpy((char *)tempData + tempLen - stringLength, &tempFloat , sizeof(float));
			tempLen += sizeof(float);
		} else if( attr.type == 2 ) {
			int tempInt; char *tempStr = (char*) malloc(100);
			// Getting value from data
			memcpy(&tempInt, (char *)data + tempLen, sizeof(int));
			memcpy(tempStr, (char *)data + tempLen + sizeof(int), tempInt); tempStr[tempInt] = '\0';

			memcpy((char *)tempData + tempLen - stringLength, &tempInt , sizeof(int));
			memcpy((char *)tempData + tempLen + sizeof(int) - stringLength, tempStr , tempInt);
			tempLen += (sizeof(int) + tempInt);
		}
	}
	return tempData;
}

/***
 * Returns the Number of string data type present in the tuple
 */
RC RM::stringInTuple(vector<Attribute> attrs)
{
	int stringCnt = 0;
	for( int i=0; i< (int)attrs.size(); i++ ) {
		Attribute attr = attrs.at(i);
		if( attr.type == 2 ) {
			stringCnt++;
		}
	}
	return stringCnt;
}
