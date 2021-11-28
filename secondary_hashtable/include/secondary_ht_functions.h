#ifndef SECONDARY_HT_FUNCTIONS_H
#define SECONDARY_HT_FUNCTIONS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include "../../BF_lib/BF.h"
#include "../../primary_hashtable/include/primary_ht.h"
#include "secondary_ht.h"


int str_hash(char *str, int number_buckets);

int print_record_by_attr_name_SHT(SHT_info header_info, Record temp_record, void *value);

int check_if_record_is_already_at_file_SHT(SHT_info header_info, void *value, int blockId);


#endif 	//SECONDARY_HT_FUNCTIONS_H