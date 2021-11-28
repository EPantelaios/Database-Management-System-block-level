#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>

#include "../include/primary_ht_functions.h"


int HT_CreateIndex(char *fileName, char attrType, char* attrName, int attrLength, int buckets){

    int fileDesc;
    void *block;
    int block_counter=0;
    int init=0;
    char metadata[18] = "Primary_Hashtable";

    HT_info ht_info;

    if(BF_CreateFile(fileName) < 0){
        BF_PrintError("Error Creating File");
        return -1;
    }

    if((fileDesc = BF_OpenFile(fileName)) < 0){
        BF_PrintError("Can't open File");
        return -1;
    }

    //Allocate first block
    if(BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Can't allocate block");
        return -1;
    }

    block_counter = BF_GetBlockCounter(fileDesc) - 1;
    assert(block_counter==0);


    if(BF_ReadBlock(fileDesc, block_counter, &block) < 0){
        BF_PrintError("Can't read block");
        return -1;
    }
    
    //the information to know if file is actually heap file
    //is a string 'HeapFile' at the first block.
    memcpy(block, metadata, strlen(metadata)+1);
    
    //copy the info from arguments to the hp_info struct
    ht_info.fileDesc = fileDesc;
    ht_info.attrType = attrType;
    strncpy(ht_info.attrName, attrName, strlen(attrName)+1);
    ht_info.attrLength = attrLength;
    ht_info.numBuckets = buckets;
    //copy information at first block
    memcpy(block+strlen(metadata)+1, &ht_info, sizeof(HT_info));

    if(BF_WriteBlock(fileDesc, block_counter) < 0){
        BF_PrintError("Can't write block");
        return -1;
    }

    
    //Create exactly 'num_of_blocks' blocks for the index 
    //Each index is an integer, so has size 4 bytes.
    //If have '1-128' indexes want only 1 bucket.
    //If have '129-256' indexes want 2 buckets ... etc
    int num_of_blocks = (((buckets * sizeof(int)) - 1) / BLOCK_SIZE) + 1;
    int indexes_each_block=0;

    //Making the index
    for(int i=0; i<num_of_blocks; i++){

        BF_AllocateBlock(fileDesc);
        block_counter = BF_GetBlockCounter(fileDesc) - 1;
        BF_ReadBlock(fileDesc, block_counter, &block);

        if(buckets < NUMBER_OF_INDEXES) //buckets < 128
            indexes_each_block = buckets;
        else
            indexes_each_block = NUMBER_OF_INDEXES;

        for(int j=0; j<indexes_each_block; j++){
            
            init=0;
            memcpy(block + j * sizeof(int), &init, sizeof(int));
        }   
        
        buckets = buckets - BLOCK_SIZE/sizeof(int);

        //save information at block
        if(BF_WriteBlock(fileDesc, block_counter) < 0){
            BF_PrintError("Can't write block");
            return -1;
        }
    }


    if(BF_CloseFile(fileDesc) < 0){
        BF_PrintError("Can't close file");
        return -1;
    }

    return 0;  
}







HT_info* HT_OpenIndex(char *fileName){

    int fileDesc;
    void *block;
    char metadata[18] = "Primary_Hashtable";
    HT_info *ht_info = (HT_info *)malloc(sizeof(HT_info));   
      

    if((fileDesc = BF_OpenFile(fileName)) < 0){
        BF_PrintError("Can't open File");
        return NULL;
    }

    //Read from first block
    if(BF_ReadBlock(fileDesc, 0, &block) < 0){
        BF_PrintError("Can't read block");
        return NULL;
    }

    //check if the first block has the correct information for hash file
    if(memcmp(metadata, block, strlen(metadata)+1)){

        printf("Error! Τhe filename that you import does not correspond to a primary hash file...");
        return NULL;
    }
   
    //copy hp_info struct from first block
    memcpy(ht_info, (block)+strlen(metadata)+1, sizeof(HT_info));


    return ht_info;
}






int HT_CloseIndex(HT_info* header_info ){

    int fileDesc = header_info->fileDesc;
    if(BF_CloseFile(fileDesc) < 0){
        BF_PrintError("Can't close file");
        return -1;
    }

    free(header_info);

    return 0;
}







int HT_InsertEntry(HT_info header_info, Record record){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int flg=0;
    int block_counter=0;
    int store_next_block=-100;
    int cnt=0;
    int hash_index=0;
    int ret_val=0;

    //check for the attribute type and name.
    //call the hash function to find the right bucket at index.
    if(header_info.attrType=='i'){

        if(!strcmp(header_info.attrName, "id")){
            hash_index = int_hash(record.id, header_info.numBuckets);
        }
        else{
            printf("Error. Insert records only with key 'ID'. Exit");
            assert(1==0);
        }
    }
    else{
        printf("Error. Indexes created with a integer key are allowed. Exit");
        assert(1==0);
    }

    //find specific block and index in the block
    int block_number = (hash_index / NUMBER_OF_INDEXES)  + 1;
    int block_index = hash_index % NUMBER_OF_INDEXES;

    BF_ReadBlock(header_info.fileDesc, block_number, &block);
    memcpy(&stored_number_of_next_block, block + block_index*sizeof(int), sizeof(int));

    //when the first record is inserted then it must create a new block
    if(stored_number_of_next_block==0){
        
        const int cnt_of_records = 1;
        const int number_next_block = -10;

        //store in the corresponding position of index the value of the current block
        block_counter=0;

        block_counter = BF_GetBlockCounter(header_info.fileDesc);
        block_counter = block_counter + 1;
        memcpy(block + block_index*sizeof(int), &block_counter, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, block_number));


        //create new block and store the record
        BF_AllocateBlock(header_info.fileDesc);
        block_counter = BF_GetBlockCounter(header_info.fileDesc) - 1;
        BF_ReadBlock(header_info.fileDesc, block_counter, &block);

        memcpy(block, &number_next_block, sizeof(int));
        memcpy(block+4, &cnt_of_records, sizeof(int));
        memcpy(block+8, &record, sizeof(Record));

        assert(!BF_WriteBlock(header_info.fileDesc, block_counter));
        
        return block_counter;
    }

    //Check for duplicate records
    if(header_info.attrType=='i'){

        if(!strcmp(header_info.attrName, "id")){
            ret_val = check_if_record_is_already_at_file(header_info,  (void*)(long int)record.id);
        }

    }


    if(ret_val==-1){

        printf("The record with key 'id  = %d, name = %s, surname = %s, address = %s' "
               "is already in the file. Not allowed to insert duplicate records.\n",
                record.id, record.name, record.surname, record.address);

        return 3;
    }
    

    BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
    memcpy(&number_next_block, block, sizeof(int));
    memcpy(&cnt_of_records, block+4, sizeof(int));


    //find the first block that is not full
    while(cnt_of_records >= (BLOCK_SIZE/sizeof(Record))){   //run only if block is full (has already 5 records).
        
        flg=1;
        cnt++;

        //If all blocks are full, then create new block     
        if(number_next_block==-10 && cnt_of_records >= (BLOCK_SIZE/sizeof(Record))){

            const int tmp_cnt_of_records = 1;
            const int tmp_number_next_block = -10;
            
            //complete the previous block's variable 'number_next_block' with the new value
            block_counter=BF_GetBlockCounter(header_info.fileDesc);
            block_counter = block_counter + 1;
            memcpy(block, &block_counter, sizeof(int));

            if(cnt!=1){
                assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));
            }
            else{
                assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));
            }


            //create new block and store the record
            BF_AllocateBlock(header_info.fileDesc);
            block_counter=BF_GetBlockCounter(header_info.fileDesc) - 1;
            BF_ReadBlock(header_info.fileDesc, block_counter, &block);

            memcpy(block, &tmp_number_next_block, sizeof(int));
            memcpy(block+4, &tmp_cnt_of_records, sizeof(int));
            memcpy(block+8, &record, sizeof(Record));

            assert(!BF_WriteBlock(header_info.fileDesc, block_counter));

            return block_counter;
        }


        store_next_block = number_next_block;
        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));
       
    }


    if(flg==0){

        memcpy(block + 8 + (cnt_of_records)*sizeof(Record), &record, sizeof(Record));
        cnt_of_records = cnt_of_records + 1;
        memcpy(block + 4, &cnt_of_records, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));

        return stored_number_of_next_block-1;
    }
    else{

        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(block + 8 + (cnt_of_records)*sizeof(Record), &record, sizeof(Record));
        cnt_of_records = cnt_of_records + 1;
        memcpy(block + 4, &cnt_of_records, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));

        return store_next_block-1;
    }
}








int HT_DeleteEntry(HT_info header_info, void *value){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int flg=0;
    int store_next_block=-100;
    int hash_index=0;
    Record temp_record;

    //check for the attribute type and name.
    //call the hash function to find the right bucket at index.
    if(header_info.attrType=='i'){

        if(!strcmp(header_info.attrName, "id")){
            hash_index = int_hash((long int)value, header_info.numBuckets);
        }
        else{
            printf("Error. Insert records only with key 'ID'. Exit");
            assert(1==0);
        }
    }
    else{
        printf("Error. Indexes created with a integer key are allowed. Exit");
        assert(1==0);
    }


    //find specific block and index in the block
    int block_number = (hash_index / NUMBER_OF_INDEXES)  + 1;
    int block_index = hash_index % NUMBER_OF_INDEXES;

    BF_ReadBlock(header_info.fileDesc, block_number, &block);
    memcpy(&stored_number_of_next_block, block + block_index*sizeof(int), sizeof(int));

    //if the current bucket of index is empty without blocks
    if(stored_number_of_next_block==0){

        printf("No record at this index of bucket.\n");        
        return -1;
    }
    
    BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
    memcpy(&number_next_block, block, sizeof(int));
    memcpy(&cnt_of_records, block+4, sizeof(int));


    //search all blocks except last for the record
    while(number_next_block != -10){

        for(int i=0; i<cnt_of_records; i++){

            memcpy(&temp_record, block + 8 + i*sizeof(Record), sizeof(Record));
            
            if((long int)value == temp_record.id){
                
                //In case of have only one record or the current record is in the last position at block
                if(cnt_of_records==1 || cnt_of_records==i+1){
                
                    memset(block + 8 + i*sizeof(Record), 0, sizeof(Record));
                }
                else{
                
                    Record record;
                    memcpy(&record, block + 8 + (cnt_of_records-1)*sizeof(Record), sizeof(Record));
                    memset(block + 8 + (cnt_of_records-1)*sizeof(Record), 0, sizeof(Record));
                    memcpy(block + 8 + i*sizeof(Record), &record, sizeof(Record));
                }

                
                if(flg==0){

                    cnt_of_records = cnt_of_records -1;
                    memcpy(block + 4, &cnt_of_records, sizeof(int));
                    assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));
                }   
                else{

                    BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
                    cnt_of_records = cnt_of_records -1;
                    memcpy(block + 4, &cnt_of_records, sizeof(int));
                    assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));
                }               


                printf("The record with ID = %lu has been deleted successfully\n", (long int)value);
                return 0;                                                     
            }
            
        }
        
        store_next_block=number_next_block;
        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));  
        memcpy(&cnt_of_records, block+4, sizeof(int));
        flg=1;
    }


    //Check last block
    for(int i=0; i<cnt_of_records; i++){

        memcpy(&temp_record, block + 8 + i*sizeof(Record), sizeof(Record));
        
        if((long int)value == temp_record.id){

            //In case of have only one record or the current record is in the last position at block
            if(cnt_of_records==1 || cnt_of_records==i+1){
            
                memset(block + 8 + i*sizeof(Record), 0, sizeof(Record));
            }
            else{
            
                Record record;
                memcpy(&record, block + 8 + (cnt_of_records-1)*sizeof(Record), sizeof(Record));
                memset(block + 8 + (cnt_of_records-1)*sizeof(Record), 0, sizeof(Record));
                memcpy(block + 8 + i*sizeof(Record), &record, sizeof(Record));
            }


            if(flg==0){
                
                cnt_of_records = cnt_of_records -1;
                memcpy(block + 4, &cnt_of_records, sizeof(int));
                assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));
            }   
            else{

                BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
                cnt_of_records = cnt_of_records -1;
                memcpy(block + 4, &cnt_of_records, sizeof(int));
                assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));
            }  

            printf("The record with ID = %lu has been deleted successfully\n", (long int)value);
            return 0;
        }      


       
    }

    //in case of not found the record
    if(header_info.attrType=='i')
        printf("There is not record with ID = %lu to delete\n", (long int)value);
    else
        printf("There is not record with value = '%s' to delete\n", (char *)value);

    return -1;
}








int HT_GetAllEntries(HT_info header_info, void *value){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int block_number = 0;
    int hash_index=0;
    Record *temp_record = (Record *)malloc(sizeof(Record));
    int cnt=1;
    int flg=0;
    int find_record=0;

    
    if(value==NULL){
        
        for(int i=0; i<header_info.numBuckets; i++){
        
            block_number = i/NUMBER_OF_INDEXES + 1;

            BF_ReadBlock(header_info.fileDesc, block_number, &block);
            memcpy(&stored_number_of_next_block, block + (i%NUMBER_OF_INDEXES)*sizeof(int), sizeof(int));

            //if the current index is empty without blocks
            if(stored_number_of_next_block==0){
                
                continue;
            }

            BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
            memcpy(&number_next_block, block, sizeof(int));
            memcpy(&cnt_of_records, block+4, sizeof(int));

            //print all blocks except last 
            while(number_next_block != -10){
                

                for(int i=0; i<cnt_of_records; i++){

                    memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));
                    print_all_records_by_attr_name(header_info, temp_record);
                }
                
                BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
                memcpy(&number_next_block, block, sizeof(int));
                memcpy(&cnt_of_records, block+4, sizeof(int));    

                cnt++;
            }
            
            //print last block
            for(int i=0; i<cnt_of_records; i++){

                memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));
                print_all_records_by_attr_name(header_info, temp_record);
            }
            cnt++;
        }
    }
    else{

        //check for the attribute type and name.
        //call the hash function to find the right bucket at index.
        if(header_info.attrType=='i'){

            if(!strcmp(header_info.attrName, "id")){
                hash_index = int_hash((long int)value, header_info.numBuckets);
            }
            else{
                printf("Error. Insert records only with key 'ID'. Exit");
                assert(1==0);
            }
        }
        else{
            printf("Error. Indexes created with a integer key are allowed. Exit");
            assert(1==0);
        }

        //find specific block and index in the block
        int block_number = (hash_index / NUMBER_OF_INDEXES)  + 1;
        int block_index = hash_index % NUMBER_OF_INDEXES;


        BF_ReadBlock(header_info.fileDesc, block_number, &block);
        memcpy(&stored_number_of_next_block, block + block_index*sizeof(int), sizeof(int));


        //if the current index is empty without blocks
        if(stored_number_of_next_block==0){

            printf("No record at this index of bucket.\n"); 
            free(temp_record);         
            return -1;
        }
        
        BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));
              

        //print all blocks except last 
        while(number_next_block != -10){
            
            for(int i=0; i<cnt_of_records; i++){

                memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));

                find_record=print_record_by_attr_name(header_info, temp_record, value);
                if(find_record==1){
                    flg=1;
                }

                if(header_info.attrType=='i'){

                    if(!strcmp(header_info.attrName, "id") && (long int)value == temp_record->id){    

                        free(temp_record);
                        return cnt;
                    }
                }
                else if(header_info.attrType=='c'){

                    if(!strcmp(header_info.attrName, "name") && !strcmp((char *)value, temp_record->name)){  

                        free(temp_record);
                        return cnt;
                    }
                    else if(!strcmp(header_info.attrName, "surname") && !strcmp((char *)value, temp_record->surname)){     

                        free(temp_record);
                        return cnt;
                    }
                    else if(!strcmp(header_info.attrName, "address") && !strcmp((char *)value, temp_record->address)){ 
                                               
                        free(temp_record);
                        return cnt;
                    }
                }
            }

            BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
            memcpy(&number_next_block, block, sizeof(int));
            memcpy(&cnt_of_records, block+4, sizeof(int));    

            cnt++;
        }
        

        //print last block
        for(int i=0; i<cnt_of_records; i++){

            memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));

            find_record=print_record_by_attr_name(header_info, temp_record, value);
            if(find_record==1){
                flg=1;
            }

            if(header_info.attrType=='i'){

                if(!strcmp(header_info.attrName, "id") && (long int)value == temp_record->id){    

                    free(temp_record);
                    return cnt;
                }
            }
            else if(header_info.attrType=='c'){

                if(!strcmp(header_info.attrName, "name") && !strcmp((char *)value, temp_record->name)){  

                    free(temp_record);
                    return cnt;
                }
                else if(!strcmp(header_info.attrName, "surname") && !strcmp((char *)value, temp_record->surname)){     

                    free(temp_record);
                    return cnt;
                }
                else if(!strcmp(header_info.attrName, "address") && !strcmp((char *)value, temp_record->address)){ 
                                            
                    free(temp_record);
                    return cnt;
                }
            }

        }

        cnt++;


        //in case of not found the record
        if(flg==0){
            
            if(header_info.attrType=='i')
                printf("The record with ID = '%ld' is not in the hash file\n", (long int)value);
            else
                printf("The record with value = '%s' is not in the hash file\n", (char *)value);
        }
        
    }
    
    free(temp_record);  
    return cnt;
}