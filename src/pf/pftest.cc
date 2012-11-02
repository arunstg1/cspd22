#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"
const int success = 0;

using namespace std;

// Check if a file exists
bool FileExists(string fileName)
{
	struct stat stFileInfo;

	if(stat(fileName.c_str(), &stFileInfo) == 0) {
		return true;
	}
	return false;
}

int PFTest_1(PF_Manager *pf)
{
	// Functions Tested:
	// 1. CreateFile
	cout << "****In PF Test Case 1****" << endl;

	RC rc;
	string fileName = "test";

	// Create a file named "test"
	rc = pf->CreateFile(fileName.c_str());
	assert(rc == success);

	if(FileExists(fileName.c_str()))
	{
		cout << "File " << fileName << " has been created." << endl << endl;
		return 0;
	}
	else
	{
		cout << "Failed to create file!" << endl;
		return -1;
	}

	// Create "test" again, should fail
	rc = pf->CreateFile(fileName.c_str());
	assert(rc != success);

	return 0;
}

int PFTest_2(PF_Manager *pf)
{
	// Functions Tested:
	// 1. OpenFile
	// 2. AppendPage
	// 3. GetNumberOfPages
	// 4. WritePage
	// 5. ReadPage
	// 6. CloseFile
	// 7. DestroyFile
	cout << "****In PF Test Case 2****" << endl;

	RC rc;
	string fileName = "test";

	// Open the file "test"
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);

	// Append the first page
	// Write ASCII characters from 32 to 125 (inclusive)
	void *data = malloc(PF_PAGE_SIZE);
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
	{
		*((char *)data+i) = i % 94 + 32;
	}
	rc = fileHandle.AppendPage(data);
	assert(rc == success);

	// Get the number of pages
	unsigned count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)1);

	// Update the first page
	// Write ASCII characters from 32 to 41 (inclusive)
	data = malloc(PF_PAGE_SIZE);
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
	{
		*((char *)data+i) = i % 10 + 32;
	}
	rc = fileHandle.WritePage(0, data);
	assert(rc == success);

	// Read the page
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(0, buffer);
	assert(rc == success);

	cout<< (char *)buffer;
	// Check the integrity
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	// Close the file "test"
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	free(data);
	free(buffer);

	// DestroyFile
	rc = pf->DestroyFile(fileName.c_str());
	assert(rc == success);

	if(!FileExists(fileName.c_str()))
	{
		cout << "File " << fileName << " has been destroyed." << endl;
		cout << "Test Case 2 Passed!" << endl << endl;
		return 0;
	}
	else
	{
		cout << "Failed to destroy file!" << endl;
		return -1;
	}
}

void pf_Test_Create(PF_Manager *pf) {

	RC rc;
	const char *filename = "mytestFile1.txt";
	cout<<endl<<"Test create"<<endl;

	// test normal file creation
	remove(filename);
	rc = pf->CreateFile(filename);
	assert (rc == success);

	// test if the code returns -1 if we try to create a file which was already existing
	rc = pf->CreateFile(filename);
	assert (rc != success);
}

void pf_Test_Open_RWops(PF_Manager *pf) {
	RC rc;
	PF_FileHandle fileHandle;
	const char *filename = "mytestFile1.txt";
	const char *filename2 = "mytestFile2.txt";

	cout<<endl<<"Test open"<<endl;
	rc = pf->OpenFile(filename, fileHandle);
	assert (rc == success);

	rc = pf->CreateFile(filename2);
	rc = pf->OpenFile(filename2, fileHandle);
	assert (rc != success);

	cout<<endl<<"Test read"<<endl;

	// A read requesting a non-existing page should fail
	void *buffer = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(20, buffer);
	assert (rc != success);

	// Append the lots of pages
	void *data = malloc(PF_PAGE_SIZE);
	for (int n = 0; n < 5000; n++){
		char ch = n % 50 + 65;

		for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
			*((char *)data+i) = ch;
		}
		rc = fileHandle.AppendPage(data);
		assert(rc == success);
	}

	// Get the number of pages
	unsigned count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)5000);

	// Check the integrity, case 1
	fileHandle.ReadPage(24, buffer);
	cout<< (char *)buffer << endl;
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = 'Y';
	}
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	// Check the integrity, case 2
	fileHandle.ReadPage(4999, buffer);
	cout<< (char *)buffer << endl;
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = 'r';
	}
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	// test write operation
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = '#';
	}
	fileHandle.WritePage(4998,data);
	fileHandle.ReadPage(4998, buffer);
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = 'r';
	}
	fileHandle.ReadPage(4999, buffer);
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	// Get the number of pages
	count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)5000);

	// Append 2 new pages and validate count
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)buffer+i) = '+';
		*((char *)data+i) = '-';
	}

	fileHandle.AppendPage(buffer);
	fileHandle.AppendPage(data);

	count = fileHandle.GetNumberOfPages();
	assert(count == (unsigned)5002);

	// Check the integrity, case 3
	fileHandle.ReadPage(4999, buffer);
	cout<< (char *)buffer << endl;
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = 'r';
	}
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	fileHandle.ReadPage(5000, buffer);
	cout<< (char *)buffer << endl;
	for(unsigned i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data+i) = '+';
	}
	rc = memcmp(data, buffer, PF_PAGE_SIZE);
	assert(rc == success);

	cout<<endl<<"Test close"<<endl;
	rc = pf->CloseFile(fileHandle);
	assert (rc == success);
}

void pf_Test_Destroy(PF_Manager *pf) {

	RC rc;
	const char *filename = "mytestFile1.txt";

	cout<<endl<<"Test destroy"<<endl;
	rc = pf->DestroyFile(filename);
	assert (rc == success);

	rc= pf->DestroyFile(filename);
	assert (rc != success);
}
