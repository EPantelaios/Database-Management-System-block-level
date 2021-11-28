#include "../include/primary_ht_functions.h"


int int_hash(int id, int number_buckets){

    return ((id*34115+97328)%947503) % number_buckets;
}




int print_record_by_attr_name(HT_info header_info, Record *temp_record, void *value){
    
    if(!strcmp(header_info.attrName, "id")){
            
        if((long int)value == temp_record->id){

            printf("id  = %d, name = %s, surname = %s, address = %s\n",
                temp_record->id, temp_record->name, temp_record->surname, temp_record->address);

            return 1;
        }
    }    
    else if(!strcmp(header_info.attrName, "name")){
            
        if(!strcmp(value, temp_record->name)){

            printf("id  = %d, name = %s, surname = %s, address = %s\n", 
                temp_record->id, temp_record->name, temp_record->surname, temp_record->address);  

            return 1;
        }
    }
    else if(!strcmp(header_info.attrName, "surname")){
    
        if(!strcmp(value, temp_record->surname)){

           
            printf("id  = %d, name = %s, surname = %s, address = %s\n", 
                temp_record->id, temp_record->name, temp_record->surname, temp_record->address);

            return 1;
        }
    }
    else if(!strcmp(header_info.attrName, "address")){

        if(!strcmp(value, temp_record->address)){

            printf("id  = %d, name = %s, surname = %s, address = %s\n", 
                temp_record->id, temp_record->name, temp_record->surname, temp_record->address);

            return 1;
        }
    }

    return 0;
}




int print_all_records_by_attr_name(HT_info header_info, Record *temp_record){

    if(!strcmp(header_info.attrName, "id")){
    
        printf("id  = %d, name = %s, surname = %s, address = %s\n",
            temp_record->id, temp_record->name, temp_record->surname, temp_record->address);
        
    }    
    else if(!strcmp(header_info.attrName, "name")){
            
        printf("id  = %d, name = %s, surname = %s, address = %s\n", 
            temp_record->id, temp_record->name, temp_record->surname, temp_record->address);  
        
    }
    else if(!strcmp(header_info.attrName, "surname")){
    
        
        printf("id  = %d, name = %s, surname = %s, address = %s\n", 
            temp_record->id, temp_record->name, temp_record->surname, temp_record->address);
    
    }
    else if(!strcmp(header_info.attrName, "address")){

        printf("id  = %d, name = %s, surname = %s, address = %s\n", 
            temp_record->id, temp_record->name, temp_record->surname, temp_record->address);
        
    }

    return 0;
}




int check_if_record_is_already_at_file(HT_info header_info, void *value){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int hash_index=0;
    Record *temp_record = (Record *)malloc(sizeof(Record));

    //call hash function to find the index
    if(header_info.attrType=='i'){

        if(!strcmp(header_info.attrName, "id")){
            hash_index = int_hash((long int)value, header_info.numBuckets);
        }
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

    //search all blocks except last 
    while(number_next_block != -10){
        

        for(int i=0; i<cnt_of_records; i++){

            memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));
            
            if(header_info.attrType=='i'){

               if(!strcmp(header_info.attrName, "id") && (long int)value == temp_record->id){                        
                    free(temp_record);
                    return -1;
                }
            }
        }

        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));    
    }
    

    //search last block
    for(int i=0; i<cnt_of_records; i++){

        memcpy(temp_record, block + 8 + i*sizeof(Record), sizeof(Record));

        if(header_info.attrType=='i'){

            if(!strcmp(header_info.attrName, "id") && (long int)value == temp_record->id){                        
                free(temp_record);
                return -1;
            }
        }
    }

    free(temp_record);  
    return 0;
}





int HashStatistics(char* filename){

    void *block;
    int block_number=0, stored_number_of_next_block=0, cnt=0, flg=0;
    int number_next_block=0, cnt_of_records=0, total_cnt_records=0;
    Record *temp_record = (Record *)malloc(sizeof(Record));
    int total_blocks=0;
    int min_cnt_records=INT_MAX, max_cnt_records=0;
    int buckets_with_overflow_blocks=0;
    int *overflow_blocks_each_bucket=malloc(5000*sizeof(int));
    char hash_file_name[100];


    //initialize array with zero.
    for(int i=0;i<5000;i++){
        overflow_blocks_each_bucket[i]=0;
    }

    int fileDesc = BF_OpenFile(filename);
    //Read metadata from first block to check 
    //if is primary or secondary hashtable
    if(BF_ReadBlock(fileDesc, 0, &block) < 0){
        BF_PrintError("Can't read block");
        return -1;
    }

    memcpy(hash_file_name, block, 21);


    //For primary hashtable
    if(!strncmp(hash_file_name, "Primary_Hashtable", 19)){

        HT_info *header_info =  HT_OpenIndex(filename);

        for(int i=0; i<header_info->numBuckets; i++){
        
            block_number = i/NUMBER_OF_INDEXES + 1;

            BF_ReadBlock(header_info->fileDesc, block_number, &block);
            memcpy(&stored_number_of_next_block, block + (i%NUMBER_OF_INDEXES)*sizeof(int), sizeof(int));

            //skip if the current index is empty without blocks
            if(stored_number_of_next_block==0){

                continue;
            }

            BF_ReadBlock(header_info->fileDesc, stored_number_of_next_block-1, &block);
            memcpy(&number_next_block, block, sizeof(int));
            memcpy(&cnt_of_records, block+4, sizeof(int));

            cnt=0;
            flg=0;

            //find all blocks except last 
            while(number_next_block != -10){
                

                total_cnt_records += cnt_of_records;
                cnt += cnt_of_records;
                overflow_blocks_each_bucket[i]++;
                flg=1;
                
                BF_ReadBlock(header_info->fileDesc, number_next_block-1, &block);
                memcpy(&number_next_block, block, sizeof(int));
                memcpy(&cnt_of_records, block+4, sizeof(int));    
            }
            
            //for last block
            total_cnt_records += cnt_of_records;
            cnt += cnt_of_records;

            if(flg==1){
                buckets_with_overflow_blocks++;
            }

            if(min_cnt_records>cnt)
                min_cnt_records=cnt;
            
            if(max_cnt_records<cnt)
                max_cnt_records=cnt; 
        }


        printf("\n\n\n\n\n---> Statistics for the Primary Hash File <---\n");

        total_blocks = BF_GetBlockCounter(header_info->fileDesc);
        printf("\nTotal Blocks: %d\n\n", total_blocks);

        printf("Records per bucket:\n>> Average: %f\n>> Minimum: %d\n>> Maximum: %d\n\n", (float)total_cnt_records/header_info->numBuckets, min_cnt_records, max_cnt_records);

        printf("Average blocks per bucket: %f\n\n",(float)(total_blocks-1)/header_info->numBuckets);

        printf("Number of buckets with at least one overflow block: %d\n", buckets_with_overflow_blocks);
        for (int i = 0; i<header_info->numBuckets;i++){

            if(overflow_blocks_each_bucket[i]>0){
                printf("Bucket %d has %d overflow blocks\n", i+1, overflow_blocks_each_bucket[i]);
            }
        }


        free(overflow_blocks_each_bucket);
        free(temp_record);

        return 0;
    }
    else if(!strncmp(hash_file_name, "Secondary_Hashtable", 21)){   //For secondary hashtable

        SHT_info *header_info =  SHT_OpenSecondaryIndex(filename);

        for(int i=0; i<header_info->numBuckets; i++){
        
            block_number = i/NUMBER_OF_INDEXES + 1;

            BF_ReadBlock(header_info->fileDesc, block_number, &block);
            memcpy(&stored_number_of_next_block, block + (i%NUMBER_OF_INDEXES)*sizeof(int), sizeof(int));

            //skip if the current index is empty without blocks
            if(stored_number_of_next_block==0){
                
                continue;
            }

            BF_ReadBlock(header_info->fileDesc, stored_number_of_next_block-1, &block);
            memcpy(&number_next_block, block, sizeof(int));
            memcpy(&cnt_of_records, block+4, sizeof(int));

            cnt=0;
            flg=0;

            //find all blocks except last 
            while(number_next_block != -10){
                

                total_cnt_records += cnt_of_records;
                cnt += cnt_of_records;
                overflow_blocks_each_bucket[i]++;
                flg=1;
                
                BF_ReadBlock(header_info->fileDesc, number_next_block-1, &block);
                memcpy(&number_next_block, block, sizeof(int));
                memcpy(&cnt_of_records, block+4, sizeof(int));    
            }
            
            //for last block
            total_cnt_records += cnt_of_records;
            cnt += cnt_of_records;

            if(flg==1){
                buckets_with_overflow_blocks++;
            }

            if(min_cnt_records>cnt)
                min_cnt_records=cnt;
            
            if(max_cnt_records<cnt)
                max_cnt_records=cnt; 
        }


        printf("\n\n\n\n\n---> Statistics for the Secondary Hash File <---\n");

        total_blocks = BF_GetBlockCounter(header_info->fileDesc);
        printf("\nTotal Blocks: %d\n\n", total_blocks);

        printf("Records per bucket:\n>> Average: %f\n>> Minimum: %d\n>> Maximum: %d\n\n", (float)total_cnt_records/header_info->numBuckets, min_cnt_records, max_cnt_records);

        printf("Average blocks per bucket: %f\n\n",(float)(total_blocks-1)/header_info->numBuckets);

        printf("Number of buckets with at least one overflow block: %d\n", buckets_with_overflow_blocks);
        for (int i = 0; i<header_info->numBuckets;i++){

            if(overflow_blocks_each_bucket[i]>0){
                printf("Bucket %d has %d overflow blocks\n", i+1, overflow_blocks_each_bucket[i]);
            }
        }


        free(overflow_blocks_each_bucket);
        free(temp_record);


        return 0;
    }
    else{

        printf("Error. Filename '%s' is not neither primary hashtable nor secondary hashtable.\n", filename);
        return -1;
    }
}