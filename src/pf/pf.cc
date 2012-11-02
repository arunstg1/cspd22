#include "pf.h"

PF_Manager* PF_Manager::_pf_manager = 0;

PF_Manager* PF_Manager::Instance()
{
	if(!_pf_manager)
		_pf_manager = new PF_Manager();

	return _pf_manager;
}

PF_Manager::PF_Manager()
{
}

PF_Manager::~PF_Manager()
{
}

bool PF_Manager::CheckFileExistence(const char *filename)
{
	ifstream pf_file(filename);
	pf_file.close();
	return pf_file;
}

RC PF_Manager::CreateFile(const char *fileName)
{
	if (CheckFileExistence(fileName)) {
		cout << "Error : The file " << fileName << " already exists." << endl;
		return -1;
	}

	ofstream pf_file(fileName, ios::binary | ios::out);
	pf_file.close();
	if (CheckFileExistence(fileName)) {
		cout << "File creation successful for " << fileName << endl;
		return 0;
	}
	cout << "Error while creating the file: " << fileName << endl;
	return -1;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	if ( ! CheckFileExistence(fileName)) {
		cout << "Error : No such file " << fileName << " exists."<< endl;
		return -1;
	}
	remove(fileName);
	if ( ! CheckFileExistence(fileName)) {
		cout << "File " << fileName << " successfully deleted" << endl;
		return 0;
	}
	cout << "Error while deleting the file: " << fileName << endl;
	return -1;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	if ( ! CheckFileExistence(fileName)) {
		cout << "Error : No such file " << fileName << " exists."<< endl;
		return -1;
	} else if (fileHandle.GetHandle()->is_open()) {
		cout<< "Error : The file handle is pointing to an opened file. Please close it first." << endl;
		return -1;
	}
	fileHandle.GetHandle()->open(fileName, ios::binary | ios::in | ios::out);

	if(fileHandle.GetHandle()->is_open()) {
		cout <<"File " << fileName << " opened successfully."<< endl;
		return 0;
	}
	cout << "Error : Cannot open file " << fileName << endl;
	return -1;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	if (fileHandle.GetHandle()->is_open()) {				// before closing, verify if a file is open
		fileHandle.GetHandle()->flush();
		fileHandle.GetHandle()->close();
	}
	return 0;
}


PF_FileHandle::PF_FileHandle()
{
	totalPages = 0;
}


PF_FileHandle::~PF_FileHandle()
{
	if (PF_handle.is_open()) {				// before destroying, close the file if it was open
		PF_handle.flush();
		PF_handle.close();
	}
}

RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if (PF_handle.is_open() && // before reading, verify if a file is open
			pageNum >= 0 && pageNum < GetNumberOfPages()) {
		PF_handle.seekg( PF_PAGE_SIZE*pageNum , ios::beg);
		PF_handle.read((char *) data, PF_PAGE_SIZE);
		PF_handle.flush();
		return 0;
	}
	return -1;
}

RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if (PF_handle.is_open() &&		// verify if a file is open
			pageNum >= 0 && pageNum <= GetNumberOfPages()) {
		PF_handle.seekp( PF_PAGE_SIZE*pageNum , ios::beg);
		PF_handle.write((char *) data, PF_PAGE_SIZE);
		PF_handle.flush();
		return 0;
	}
	return -1;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	if (PF_handle.is_open()) { 				// verify if a file is open
		PF_handle.seekp(0,ios::end);
		PF_handle.write((char *) data, PF_PAGE_SIZE);
		PF_handle.flush();
		return 0;
	}
	return -1;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	if (PF_handle.is_open()) {				// verify if a file is open
		PF_handle.seekg( 0, ios::end);
		long endPos = PF_handle.tellg();
		long pages = endPos / (long)PF_PAGE_SIZE;
		return (unsigned) pages;
	}
	return -1;
}

fstream * PF_FileHandle::GetHandle()
{
	return &PF_handle;
}
