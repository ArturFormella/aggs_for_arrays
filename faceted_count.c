
// before uthash
#define HASH_FUNCTION(key,keylen,hashv)                                       \
{                                                                             \
  (hashv) = *(key);                                                           \
}

#include "uthash.h"
//#include "lib/stringinfo.h"
#include "libpq/pqformat.h"

Datum faceted_count_transfn(PG_FUNCTION_ARGS);
Datum faceted_count_serial(PG_FUNCTION_ARGS);
Datum faceted_count_deserial(PG_FUNCTION_ARGS);
Datum faceted_count_combine(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(faceted_count_transfn);
PG_FUNCTION_INFO_V1(faceted_count_serial);    // parallel aggregation support functions
PG_FUNCTION_INFO_V1(faceted_count_deserial);
PG_FUNCTION_INFO_V1(faceted_count_combine);

#define CHECK_AGG_CONTEXT(fname, fcinfo)  \
    if (! AggCheckCallContext(fcinfo, NULL)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
}
#define GET_AGG_CONTEXT(fname, fcinfo, aggcontext)  \
    if (! AggCheckCallContext(fcinfo, &aggcontext)) {   \
        elog(ERROR, "%s called in non-aggregate context", fname);  \
}
typedef struct hashMap {
    int id;            /* key */
    int value;
    UT_hash_handle hh; /* makes this structure hashable */
} hashMap;

typedef struct VecArrayBuildState2 {
  Oid inputElementType;
  hashMap *keys;
} VecArrayBuildState2;

void delete_all(hashMap *users);
void delete_all(hashMap *users)
{
    hashMap *current_user, *tmp;

    HASH_ITER(hh, users, current_user, tmp) {
        HASH_DEL(users,current_user);  // delete it (users advances to next)
        free(current_user);            //  free it
    }
}

ArrayType  * createArray(VecArrayBuildState2 *state);
ArrayType  * createArray(VecArrayBuildState2 *state)
{
  int i = 0;
  int size = HASH_COUNT(state->keys);
  int dims[2];
  int lbs[2];
  char elemTypeAlignmentCode;
  int real_size  = size*2;  
  int16 elemTypeWidth;
  ArrayType  *result;
  bool elemTypeByValue;
  hashMap *s;
  Datum elements[real_size];

  elog(WARNING, "-----------------" );
 // elog(WARNING, "  size %d", size );
  for(s=state->keys; s != NULL; s=(hashMap*)(s->hh.next)) {
  //  elog(WARNING, "  user id %d: value %d", s->id, s->value);
    elements[i] = s->id;
    elements[i + size] = s->value;
    i++;
  }
  dims[0] = 2;    // 2x bigger
  dims[1] = size;
  lbs[0] = 1;
  lbs[1] = 1;

  get_typlenbyvalalign(INT4OID, &elemTypeWidth, &elemTypeByValue, &elemTypeAlignmentCode);

  result = construct_md_array(elements, NULL, 2, dims, lbs, INT4OID, elemTypeWidth, elemTypeByValue, elemTypeAlignmentCode);

  return result;
}

Datum
faceted_count_transfn(PG_FUNCTION_ARGS)
{
  int i;
  hashMap *s;
  int32 searchForKey;
  bool *currentNulls;
  Oid elemTypeId;
  int16 elemTypeWidth;
  bool elemTypeByValue;
  char elemTypeAlignmentCode;
  int currentLength;
  MemoryContext aggContext;
  VecArrayBuildState2 *state = NULL;
  ArrayType *currentArray;
  Datum *currentVals;

  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "faceted_count_transfn called in non-aggregate context");
  }

  // PG_ARGISNULL tests for SQL NULL,
  // but after the first pass we can have a
  // value that is non-SQL-NULL but still is C NULL.
  if (!PG_ARGISNULL(0)) {
    state = (VecArrayBuildState2 *)PG_GETARG_POINTER(0);
  }

  if (PG_ARGISNULL(1)) {
    // just return the current state unchanged (possibly still NULL)
    PG_RETURN_POINTER(state);
  }
  currentArray = PG_GETARG_ARRAYTYPE_P(1);

  if (state == NULL) {
    // Since we have our first not-null argument
    // we can initialize the state to match its length.
    elemTypeId = ARR_ELEMTYPE(currentArray);
    if (elemTypeId != INT2OID &&
        elemTypeId != INT4OID &&
        elemTypeId != INT8OID ) {
      ereport(ERROR, (errmsg("faceted_count input must be array of SMALLINT, INTEGER or BIGINT")));
    }
    if (ARR_NDIM(currentArray) != 1) {
      ereport(ERROR, (errmsg("One-dimensional arrays are required")));
    }
    state = (VecArrayBuildState2 *)MemoryContextAlloc(aggContext, sizeof(VecArrayBuildState2));
    state->inputElementType = elemTypeId;
    state->keys = NULL;
  } else {
    elemTypeId = state->inputElementType;
  }

  get_typlenbyvalalign(elemTypeId, &elemTypeWidth, &elemTypeByValue, &elemTypeAlignmentCode);
  deconstruct_array(currentArray, elemTypeId, elemTypeWidth, elemTypeByValue, elemTypeAlignmentCode,&currentVals, &currentNulls, &currentLength);

  for (i = 0; i < currentLength; i++) {
    switch (elemTypeId) {
      case INT2OID:
        searchForKey= DatumGetInt16(currentVals[i]);
        break;
      case INT4OID:
        searchForKey= DatumGetInt32(currentVals[i]);
        break;
      case INT8OID:
        searchForKey= DatumGetInt64(currentVals[i]);
        break;
      default:
        elog(ERROR, "Unknown elemTypeId!");
      }
    //elog(WARNING, "search: searchForKey: %d, currentLength: %d", searchForKey, currentLength);

    HASH_FIND_INT( state->keys, &searchForKey, s );// each element is an ID of tag, add +1 for every ID

    if(s == NULL){
      s = (hashMap*)malloc(sizeof(hashMap));
       if(s == NULL){
         elog(ERROR, "malloc error!"); 
       }else{
        s->value = 1;
        s->id = searchForKey;
        HASH_ADD_INT( state->keys, id, s );
      }
    }else{
      s->value++;
    }
  }
  PG_RETURN_POINTER(state);
}

Datum faceted_count_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(faceted_count_finalfn);

Datum
faceted_count_finalfn(PG_FUNCTION_ARGS)
{
  ArrayType  * result;
  VecArrayBuildState2 *state;

  Assert(AggCheckCallContext(fcinfo, NULL));

  state = PG_ARGISNULL(0) ? NULL : (VecArrayBuildState2 *)PG_GETARG_POINTER(0);

  if (state == NULL){
    PG_RETURN_NULL();
  }
  result = createArray(state);
  delete_all(state->keys);
  PG_RETURN_ARRAYTYPE_P(result);
}

Datum
faceted_count_serial(PG_FUNCTION_ARGS)
{
    VecArrayBuildState2 * state = (VecArrayBuildState2 *)PG_GETARG_POINTER(0);
    bytea  *result;                                    /* output */
    hashMap *s2;
    int size;
    StringInfoData buf;
    // Assert(state != NULL);
    CHECK_AGG_CONTEXT("faceted_count_serial", fcinfo);
/*

    result = (bytea *)palloc(VARHDRSZ + dlen + hlen);

    SET_VARSIZE(result, VARHDRSZ + dlen + hlen);
    ptr = VARDATA(result);

    memcpy(ptr, state, hlen);
    ptr += hlen;

    memcpy(ptr, state->data, dlen);
*/

    pq_begintypsend(&buf);

    size = HASH_COUNT(state->keys);
    pq_sendint(&buf, state->inputElementType, 4);
    pq_sendint(&buf, size, 4);

    elog(WARNING, "  serial size: %d", size);        // tyle liczb 8-bajtowych

    /* merge the two arrays */
    for(s2=state->keys; s2 != NULL; s2=(hashMap*)(s2->hh.next)) {
        //  elog(WARNING, "  user id %d: value %d", s2->id, s2->value);
        pq_sendint(&buf, s2->id, 4);
        pq_sendint(&buf, s2->value, 4);
    }
    result = pq_endtypsend(&buf);
    PG_RETURN_BYTEA_P(result);
}

Datum
faceted_count_deserial(PG_FUNCTION_ARGS)
{
    bytea  *stateByte;
    MemoryContext aggContext;
    VecArrayBuildState2 *state;
    int 
        i,
        j, size;
    hashMap *s;
    StringInfoData buf;

    GET_AGG_CONTEXT("faceted_count_deserial", fcinfo, aggContext);
    state = (VecArrayBuildState2 *)MemoryContextAlloc(aggContext, sizeof(VecArrayBuildState2));
    state->inputElementType = INT4OID;//elemTypeId;
    state->keys = NULL;
    stateByte = (bytea *)PG_GETARG_POINTER(0);

    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, VARDATA(stateByte), VARSIZE(stateByte) - VARHDRSZ);

    state->inputElementType = pq_getmsgint(&buf, 4);
    size = pq_getmsgint(&buf, 4);
    j=0;
//    length = (VARSIZE(stateByte) - VARHDRSZ - 2 )/ 2 /sizeof(int32);        // 2 = header size, 2 becouse it is list of pairs
    elog(WARNING, "    odczyt size: %d",size);

    for (i = 0; i<size; i++) {
        int32 key = pq_getmsgint(&buf, 4);
        int32 value = pq_getmsgint(&buf, 4);

          s = (hashMap*)malloc(sizeof(hashMap));
          if(s == NULL){
             elog(ERROR, "malloc error!"); 
          }else{
            s->value = value;
            s->id = key;
            HASH_ADD_INT( state->keys, id, s );
          }
        j++;
    //    elog(WARNING, "  przyszlo: %ld (%d)", key, i );
    }
//    elog(WARNING, "  size: %d %d %d", VARSIZE(stateByte) - VARHDRSZ, size, j);
    // CHECK_AGG_CONTEXT("count_distinct_deserial", fcinfo);
 //   memcpy(state, ptr, offsetof(VecArrayBuildState2, data));
 //   ptr += offsetof(VecArrayBuildState2, data);

    PG_RETURN_POINTER(state);
}

Datum
faceted_count_combine(PG_FUNCTION_ARGS)
{
    int searchForKey;
    int s1;
    VecArrayBuildState2 *state1;
    VecArrayBuildState2 *state2;
    MemoryContext agg_context;
    MemoryContext old_context;
    hashMap *s, *s2;

    GET_AGG_CONTEXT("faceted_count_combine", fcinfo, agg_context);

    state1 = PG_ARGISNULL(0) ? NULL : (VecArrayBuildState2 *) PG_GETARG_POINTER(0);
    state2 = PG_ARGISNULL(1) ? NULL : (VecArrayBuildState2 *) PG_GETARG_POINTER(1);

    if (state2 == NULL){
        elog(WARNING, "connect only A: %d", HASH_COUNT(state1->keys));
        PG_RETURN_POINTER(state1);
    }
    old_context = MemoryContextSwitchTo(agg_context);
    if (state1 == NULL)
    {
        state1 = (VecArrayBuildState2 *)MemoryContextAlloc(agg_context, sizeof(VecArrayBuildState2));
        state1->inputElementType = INT4OID;//elemTypeId;
        state1->keys = NULL;

        if(state2 != NULL){        // just copy data from state2 to state1
            elog(WARNING, "connect only B: %d", HASH_COUNT(state2->keys));
        }

  //      state1->data = palloc(state1->nbytes);
  //      memcpy(state1->data, state2->data, state1->nbytes);

            for(s2=state2->keys; s2 != NULL; s2=(hashMap*)(s2->hh.next)) {
                //  elog(WARNING, "  user id %d: value %d", s2->id, s2->value);
              s = (hashMap*)malloc(sizeof(hashMap));
              if(s == NULL){
                 elog(ERROR, "malloc error!"); 
              }else{
                s->value = s2->value;
                s->id = s2->id;
                HASH_ADD_INT( state1->keys, id, s );
              }
            }
        MemoryContextSwitchTo(old_context);

        PG_RETURN_POINTER(state1);
    }

    state1->inputElementType = state2->inputElementType;

    s1 = HASH_COUNT(state1->keys);

    /* merge the two arrays */
    for(s2=state2->keys; s2 != NULL; s2=(hashMap*)(s2->hh.next)) {
        //  elog(WARNING, "  user id %d: value %d", s2->id, s2->value);
        searchForKey = s2->id;
        HASH_FIND_INT( state1->keys, &searchForKey, s );// each element is an ID of tag, add +1 for every ID

        if(s == NULL){
          s = (hashMap*)malloc(sizeof(hashMap));
          if(s == NULL){
             elog(ERROR, "malloc error!"); 
          }else{
            s->value = s2->value;
            s->id = searchForKey;
            HASH_ADD_INT( state1->keys, id, s );
          }
        }else{
          s->value += s2->value;
        }
    }
    elog(WARNING, "connect %d + %d = %d", s1, HASH_COUNT(state2->keys), HASH_COUNT(state1->keys) );

   //  data = MemoryContextAlloc(agg_context, (state1->nbytes + state2->nbytes));
   //  tmp = data;
   
    MemoryContextSwitchTo(old_context);

    PG_RETURN_POINTER(state1);
}
