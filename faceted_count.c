
#include "uthash.h"

Datum faceted_count_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(faceted_count_transfn);

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

  // elog(WARNING, "  size %d", size );
  for(s=state->keys; s != NULL; s=(hashMap*)(s->hh.next)) {
  //  elog(WARNING, "  user id %d: value %d", s->id, s->value);
    elements[i] = s->id;
    elements[i + size] = s->value;
    i++;
  }
  dims[0] = 2;	// 2x bigger
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
      if(s != NULL){
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

