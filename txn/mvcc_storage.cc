// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
    // std::cout << "inside loop" << std::endl << std::flush;

  }
  // std::cout << "Test init storage" << std::endl << std::flush;
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
  bool found = false;
  if (mvcc_data_.count(key) && !(*mvcc_data_[key]).empty()){ 
    deque<Version*> versions = *mvcc_data_[key];
    int temp = -1;
    for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
      if (((*it)->version_id_ > temp) && ((*it)->version_id_<=txn_unique_id)){
        temp = (*it)->version_id_;
        *result = (*it)->value_;
        found = true;
      }
    }
    for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
      if (((*it)->max_read_id_ < txn_unique_id) && (temp == (*it)->version_id_)) {
      (*it)->max_read_id_ = txn_unique_id;
    }

    }
  }
  // std::cout << "Read found : " << found << std::endl; 
  return found;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  if (mvcc_data_.count(key) && !(*mvcc_data_[key]).empty()){
    deque<Version*> versions = *mvcc_data_[key];
    int temp_write = -1;
    int temp_read = -1;
    for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
      if (((*it)->version_id_ > temp_write) && ((*it)->version_id_<=txn_unique_id)){
        temp_write = (*it)->version_id_;
        temp_read = (*it)->max_read_id_;
      }
    }
    if (temp_read > txn_unique_id){
      return false;
    }
    else{
      return true;
    }
  }
  else {
    return true;
  }

}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  // std::cout << "Test write" << std::endl << std::flush;
  if (!mvcc_data_.count(key)){
    // std::cout << "Test write no key exist in mvcc data" << std::endl << std::flush;
    deque<Version*>* versions = new deque<Version*>;
    mvcc_data_[key] = versions;
    // std::cout << "Test write add key and value" << std::endl << std::flush;

    Version *new_ver = new Version;
    (new_ver)->value_ = value;
    (new_ver)->max_read_id_ = txn_unique_id;
    (new_ver)->version_id_ = txn_unique_id;
    mvcc_data_[key]->push_back(new_ver);
    // std::cout << mvcc_data_[key]->size() << std::endl << std::flush;
    }
  else {
    deque<Version*> versions = *mvcc_data_[key];
    if (!versions.empty()){
      int temp = -1;
      for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
        if (((*it)->version_id_ > temp) && ((*it)->version_id_<=txn_unique_id)){
          temp = (*it)->version_id_;
        }
      }
      for (deque<Version*>::iterator it=versions.begin(); it!=versions.end(); ++it){
        if (temp == (*it)->version_id_){
          if (txn_unique_id == (*it)->version_id_){
            (*it)->value_ = value;
          }
          else{
            Version *new_ver = new Version;
            new_ver->value_ = value;
            new_ver->max_read_id_ = txn_unique_id;
            new_ver->version_id_ = txn_unique_id;
            mvcc_data_[key]->push_back(new_ver);
          }
        }
      }
    }
    else {
      Version *new_ver = new Version;
      new_ver->value_ = value;
      new_ver->max_read_id_ = txn_unique_id;
      new_ver->version_id_ = txn_unique_id;
      (mvcc_data_)[key]->push_back(new_ver);
    }

  }

}


