#ifndef PRIMARY_HT_FUNCTIONS_H
#define PRIMARY_HT_FUNCTIONS_H


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include "../../BF_lib/BF.h"
#include "primary_ht.h"
#include "../../secondary_hashtable/include/secondary_ht.h"


int int_hash(int id, int number_buckets);

int print_record_by_attr_name(HT_info header_info, Record *temp_record, void *value);

int print_all_records_by_attr_name(HT_info header_info, Record *temp_record);

int check_if_record_is_already_at_file(HT_info header_info, void *value);

int HashStatistics(char* filename);


#endif 	//PRIMARY_HT_FUNCTIONS_H