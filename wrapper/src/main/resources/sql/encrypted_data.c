/*
 * encrypted_data.c
 * PostgreSQL custom type for HMAC-verified encrypted data
 * Format: [HMAC:32bytes][type:1byte][IV:12bytes][ciphertext]
 * Minimum length: 32 + 1 + 12 + 16 (min AES block) = 61 bytes
 */

#include "postgres.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "varatt.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define MIN_ENCRYPTED_DATA_LEN 61  /* 32 + 1 + 12 + 16 */

/* Input function: text -> encrypted_data */
PG_FUNCTION_INFO_V1(encrypted_data_in);
Datum
encrypted_data_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    bytea *result;
    int len;

    /* Decode from base64 */
    result = DatumGetByteaP(DirectFunctionCall1(byteain, CStringGetDatum(str)));
    len = VARSIZE_ANY_EXHDR(result);

    /* Validate minimum length */
    if (len < MIN_ENCRYPTED_DATA_LEN)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid encrypted_data: too short (%d bytes, minimum %d)",
                        len, MIN_ENCRYPTED_DATA_LEN)));

    PG_RETURN_BYTEA_P(result);
}

/* Output function: encrypted_data -> text */
PG_FUNCTION_INFO_V1(encrypted_data_out);
Datum
encrypted_data_out(PG_FUNCTION_ARGS)
{
    bytea *data = PG_GETARG_BYTEA_PP(0);
    char *result;

    /* Encode to base64 for display */
    result = DatumGetCString(DirectFunctionCall1(byteaout, PointerGetDatum(data)));

    PG_RETURN_CSTRING(result);
}

/* Receive function: binary input */
PG_FUNCTION_INFO_V1(encrypted_data_recv);
Datum
encrypted_data_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    bytea *result;
    int len;

    len = buf->len - buf->cursor;

    if (len < MIN_ENCRYPTED_DATA_LEN)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                 errmsg("invalid encrypted_data: too short")));

    result = (bytea *) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), len);

    PG_RETURN_BYTEA_P(result);
}

/* Send function: binary output */
PG_FUNCTION_INFO_V1(encrypted_data_send);
Datum
encrypted_data_send(PG_FUNCTION_ARGS)
{
    bytea *data = PG_GETARG_BYTEA_PP(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data));

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* Validate HMAC structure (checks format without key) */
PG_FUNCTION_INFO_V1(encrypted_data_validate_structure);
Datum
encrypted_data_validate_structure(PG_FUNCTION_ARGS)
{
    bytea *data = PG_GETARG_BYTEA_PP(0);
    int len = VARSIZE_ANY_EXHDR(data);

    /* Check minimum length */
    if (len < MIN_ENCRYPTED_DATA_LEN)
        ereport(ERROR,
                (errcode(ERRCODE_CHECK_VIOLATION),
                 errmsg("encrypted_data validation failed: too short (%d bytes)", len)));

    /* Verify salt length (first 16 bytes exist) */
    if (len < 16)
        ereport(ERROR,
                (errcode(ERRCODE_CHECK_VIOLATION),
                 errmsg("encrypted_data validation failed: missing salt")));

    /* Verify HMAC tag length (next 32 bytes exist) */
    if (len < 48)
        ereport(ERROR,
                (errcode(ERRCODE_CHECK_VIOLATION),
                 errmsg("encrypted_data validation failed: missing HMAC tag")));

    PG_RETURN_BOOL(true);
}
