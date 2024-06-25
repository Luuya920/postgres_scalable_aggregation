#include "postgres.h"
#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(add_hundred);

Datum add_hundred(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);

    PG_RETURN_INT32(arg + 100);
}

PG_FUNCTION_INFO_V1(generate_driving_periods);

Datum generate_driving_periods(PG_FUNCTION_ARGS)
{
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    int32 partition = PG_GETARG_INT64(1);
    JsonbIterator *it;
    JsonbValue v;
    JsonbIteratorToken r;
    StringInfo result = makeStringInfo();

    char *current_driver = NULL;
    int64 start_timestamp = 0;
    bool first_entry = true;

    it = JsonbIteratorInit(&jb->root);
    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        if (r == WJB_BEGIN_ARRAY)
        {
            while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_END_ARRAY)
            {
                if (r == WJB_BEGIN_OBJECT)
                {
                    char *driver_name = NULL;
                    int64 timestamp = 0;
                    while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_END_OBJECT)
                    {
                        if (r == WJB_KEY)
                        {
                            char *key = pnstrdup(v.val.string.val, v.val.string.len);

                            r = JsonbIteratorNext(&it, &v, false);
                            if (strcmp(key, "driver_name") == 0)
                            {
                                if (v.type == jbvString)
                                {
                                    driver_name = pnstrdup(v.val.string.val, v.val.string.len);
                                }
                            }
                            else if (strcmp(key, "timestamp") == 0)
                            {
                                if (v.type == jbvNumeric)
                                {
                                    timestamp = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(v.val.numeric)));
                                }
                            }
                            pfree(key);
                        }
                    }
                    if (first_entry)
                    {
                        current_driver = driver_name;
                        start_timestamp = timestamp;
                        first_entry = false;
                    }
                    else if (current_driver && driver_name && strcmp(current_driver, driver_name) != 0)
                    {
                        while (timestamp > start_timestamp)
                        {
                            appendStringInfo(result, "Driver %s drove at %ld\n", current_driver, start_timestamp);
                            start_timestamp += partition;
                        }
                        appendStringInfo(result, "Driver changed from %s to %s at timestamp %ld\n", current_driver, driver_name, timestamp);
                        current_driver = driver_name;
                        start_timestamp = timestamp;
                    }
                    else if (driver_name)
                    {
                        pfree(driver_name);
                    }

                    while (timestamp > start_timestamp)
                    {
                        appendStringInfo(result, "Driver %s drove at %ld\n", current_driver, start_timestamp);
                        start_timestamp += partition;
                    }
                }
            }
        }
    }
    if (current_driver)
    {
        appendStringInfo(result, "Driver %s drove at %ld\n", current_driver, start_timestamp);
    }

    PG_RETURN_TEXT_P(cstring_to_text(result->data));
}

typedef struct AvgSpeedState
{
    double sum;
    int count;
} AvgSpeedState;

PG_FUNCTION_INFO_V1(avg_speed_transfn);
PG_FUNCTION_INFO_V1(avg_speed_finalfn);
PG_FUNCTION_INFO_V1(avg_speed_combinefn);
PG_FUNCTION_INFO_V1(avg_speed_serializefn);
PG_FUNCTION_INFO_V1(avg_speed_deserializefn);

Datum avg_speed_transfn(PG_FUNCTION_ARGS);
Datum avg_speed_finalfn(PG_FUNCTION_ARGS);
Datum avg_speed_combinefn(PG_FUNCTION_ARGS);
Datum avg_speed_serializefn(PG_FUNCTION_ARGS);
Datum avg_speed_deserializefn(PG_FUNCTION_ARGS);

Datum avg_speed_transfn(PG_FUNCTION_ARGS)
{
    AvgSpeedState *state;
    MemoryContext agg_context;
    MemoryContext old_context;
    ArrayType *speeds;
    Datum *elements;
    bool *nulls;
    int num_speeds, i;

    // Check if function is called in an aggregate context and get the memory context
    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "avg_speed_transfn called in non-aggregate context");
    }

    // Switch to the aggregate memory context for allocating the state
    old_context = MemoryContextSwitchTo(agg_context);

    // Initialize or retrieve the state
    if (PG_ARGISNULL(0))
    {
        state = (AvgSpeedState *)palloc(sizeof(AvgSpeedState));
        state->sum = 0;
        state->count = 0;
    }
    else
    {
        state = (AvgSpeedState *)PG_GETARG_POINTER(0);
    }

    if (!PG_ARGISNULL(1))
    {
        speeds = PG_GETARG_ARRAYTYPE_P(1);

        if (ARR_ELEMTYPE(speeds) != FLOAT8OID)
        {
            elog(ERROR, "avg_speed_transfn expected an array of type double precision");
        }

        deconstruct_array(speeds, FLOAT8OID, 8, true, 'd', &elements, &nulls, &num_speeds);

        for (i = 0; i < num_speeds; i++)
        {
            if (!nulls[i])
            {
                double speed = DatumGetFloat8(elements[i]);
                state->sum += speed;
                state->count++;
            }
        }
    }

    // Switch back to the previous memory context
    MemoryContextSwitchTo(old_context);

    PG_RETURN_POINTER(state);
}

Datum avg_speed_finalfn(PG_FUNCTION_ARGS)
{
    AvgSpeedState *state = (AvgSpeedState *)PG_GETARG_POINTER(0);
    double avg = state->count == 0 ? 0 : state->sum / state->count;

    PG_RETURN_FLOAT8(avg);
}

Datum avg_speed_combinefn(PG_FUNCTION_ARGS)
{
    AvgSpeedState *state1;
    AvgSpeedState *state2;
    MemoryContext agg_context;
    MemoryContext old_context;

    // Check if function is called in an aggregate context and get the memory context
    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "avg_speed_combinefn called in non-aggregate context");
    }

    old_context = MemoryContextSwitchTo(agg_context);

    // Handle null states
    if (PG_ARGISNULL(0))
    {
        state1 = (AvgSpeedState *)palloc(sizeof(AvgSpeedState));
        state1->sum = 0;
        state1->count = 0;
    }
    else
    {
        state1 = (AvgSpeedState *)PG_GETARG_POINTER(0);
    }

    if (!PG_ARGISNULL(1))
    {
        state2 = (AvgSpeedState *)PG_GETARG_POINTER(1);

        // Combine the states
        state1->sum += state2->sum;
        state1->count += state2->count;
    }

    MemoryContextSwitchTo(old_context);

    PG_RETURN_POINTER(state1);
}

Datum avg_speed_serializefn(PG_FUNCTION_ARGS)
{
    AvgSpeedState *state = (AvgSpeedState *)PG_GETARG_POINTER(0);
    StringInfoData buf;
    bytea *result;

    pq_begintypsend(&buf);
    pq_sendfloat8(&buf, state->sum);
    pq_sendint32(&buf, state->count);
    result = pq_endtypsend(&buf);

    // Free the state after serialization to manage memory
    pfree(state);

    PG_RETURN_BYTEA_P(result);
}

Datum avg_speed_deserializefn(PG_FUNCTION_ARGS)
{
    bytea *sstate = PG_GETARG_BYTEA_P(0);
    AvgSpeedState *result;
    StringInfoData buf;

    result = (AvgSpeedState *)palloc(sizeof(AvgSpeedState));

    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, VARDATA(sstate), VARSIZE(sstate) - VARHDRSZ);

    result->sum = pq_getmsgfloat8(&buf);
    result->count = pq_getmsgint(&buf, sizeof(int32));

    PG_RETURN_POINTER(result);
}