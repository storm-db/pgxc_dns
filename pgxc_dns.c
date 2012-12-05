/*-------------------------------------------------------------------------
 *
 *		  pgxc_dns
 *
 * Copyright (c) 2012, StormDB, Inc.
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Jim Mlodgenski <jim@stormdb.com>
 *
 * IDENTIFICATION
 *		  pgxc_dns/pgxc_dns.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "access/hash.h"

#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/nodemgr.h"



PG_MODULE_MAGIC;

/*---- GUC variables ----*/

static int	xcdns_ttl;
static int	xcdns_zone_ttl;
static char    *xcdns_zone = NULL;
static char    *xcdns_name = NULL;
static char    *xcdns_host = NULL;

/*---- Structures ----*/
typedef struct HashKey
{
        int host_len;
        const char *host_ptr;
} HashKey;

typedef struct Entry
{
        HashKey key;         /* hash key of entry */
        int32   weight;
        char    host[NAMEDATALEN];
} Entry;



/*---- Function declarations ----*/

#define PGXC_DNS_ZONE_COLS         4
#define PGXC_DNS_HOST_WEIGHT_COLS  2

void		_PG_init(void);

Datum		pgxc_dns_a_rec_weight(PG_FUNCTION_ARGS);
Datum           pgxc_dns_host_weight(PG_FUNCTION_ARGS);
Datum		pgxc_dns_zone(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pgxc_dns_a_rec_weight);
PG_FUNCTION_INFO_V1(pgxc_dns_host_weight);
PG_FUNCTION_INFO_V1(pgxc_dns_zone);

static uint32 dns_hash_fn(const void *key, Size keysize);
static int dns_match_fn(const void *key1, const void *key2, Size keysize);

Tuplestorestate *gather_remote_coord_info(Oid funcid);
static HTAB *gather_weights(void);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomIntVariable("pgxc_dns.ttl",
	  "Sets the value of TTL returned by pgxc_dns A records.",
							NULL,
							&xcdns_ttl,
							0,
							0,
							INT_MAX,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pgxc_dns.zone_ttl",
	  "Sets the value of TTL returned by pgxc_dns zone records.",
							NULL,
							&xcdns_zone_ttl,
							3600,
							0,
							INT_MAX,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

        DefineCustomStringVariable("pgxc_dns.zone",
          "Sets the value of the zone returned by pgxc_dns.",
                                                        NULL,
                                                        &xcdns_zone,
                                                        NULL,
                                                        PGC_SUSET, 
                                                        0,
                                                        NULL, 
                                                        NULL, 
                                                        NULL);

        DefineCustomStringVariable("pgxc_dns.name",
          "Sets the value of the name returned by pgxc_dns.",
                                                        NULL,
                                                        &xcdns_name,
                                                        NULL,
                                                        PGC_SUSET, 
                                                        0,
                                                        NULL, 
                                                        NULL, 
                                                        NULL);

        DefineCustomStringVariable("pgxc_dns.host",
          "Sets the value of the host returned by pgxc_dns.",
                                                        NULL,
                                                        &xcdns_host,
                                                        NULL,
                                                        PGC_SUSET, 
                                                        0,
                                                        NULL, 
                                                        NULL, 
                                                        NULL);

	EmitWarningsOnPlaceholders("pgxc_dns");
}

Datum
pgxc_dns_zone(PG_FUNCTION_ARGS)
{
	ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext   per_query_ctx;
	MemoryContext   oldcontext;
	Datum		values[PGXC_DNS_ZONE_COLS];
	bool		nulls[PGXC_DNS_ZONE_COLS];
        Entry           *entry;
        Entry           *a_rec = NULL;
        HTAB            *LocalHash = NULL;
        HASH_SEQ_STATUS hash_seq;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

        /*--- Add SOA record ---*/
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

        if (xcdns_zone != NULL)
            values[0] = CStringGetTextDatum(xcdns_zone);
        else
            nulls[0] = true;

        values[1] = Int32GetDatum(xcdns_zone_ttl);
        values[2] = CStringGetTextDatum("SOA");

        if (xcdns_zone != NULL) {
            char value[200];
             
            /* SOA record needs to be a specific format
             * zone. email. sn refresh retry expiry ttl
             */
            sprintf(value, "%s. mail.%s. 1 %d %d %d %d", 
                       xcdns_zone,
                       xcdns_zone,
                       xcdns_zone_ttl,
                       xcdns_zone_ttl,
                       xcdns_zone_ttl,
                       xcdns_zone_ttl);
            values[3] = CStringGetTextDatum(value);
        } else
            nulls[3] = true;

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);


        /* Get the details from the cluster */
        LocalHash = gather_weights();

        /*--- Add NS records ---*/
        hash_seq_init(&hash_seq, LocalHash);
        while ((entry = hash_seq_search(&hash_seq)) != NULL) {
            memset(values, 0, sizeof(values));
            memset(nulls, 0, sizeof(nulls));

            if (xcdns_zone != NULL)
                values[0] = CStringGetTextDatum(xcdns_zone);
            else
                nulls[0] = true;

            values[1] = Int32GetDatum(xcdns_zone_ttl);
            values[2] = CStringGetTextDatum("NS");
            values[3] = CStringGetTextDatum(entry->host);

            tuplestore_putvalues(tupstore, tupdesc, values, nulls);


            /* Look for the least loaded coordinator while we're looping
             * through all of the records
             */
            if (a_rec) {
                if (entry->weight < a_rec->weight)
                    a_rec = entry; 
            } else {
                a_rec = entry;
            }
        }

        /*--- Add A record ---*/
        memset(values, 0, sizeof(values));
        memset(nulls, 0, sizeof(nulls));

        if (xcdns_zone != NULL) {
            char value[200];

            sprintf(value, "%s.%s",
                       xcdns_name,
                       xcdns_zone);
            values[0] = CStringGetTextDatum(value);
        } else
            nulls[0] = true;

        values[1] = Int32GetDatum(xcdns_ttl);
        values[2] = CStringGetTextDatum("A");
        values[3] = CStringGetTextDatum(a_rec->host);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);

        /* destroy local hash table */
        if (LocalHash)
                hash_destroy(LocalHash);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

Datum
pgxc_dns_host_weight(PG_FUNCTION_ARGS)
{
        ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
        TupleDesc        tupdesc;
        Tuplestorestate *tupstore = NULL;
        MemoryContext    per_query_ctx;
        MemoryContext    oldcontext;
        Datum            values[PGXC_DNS_HOST_WEIGHT_COLS];
        bool             nulls[PGXC_DNS_HOST_WEIGHT_COLS];
        int              backends = 0;
        int              weight = 0;

        if (IS_PGXC_DATANODE)
                ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("invalid invocation on data node")));

        /* check to see if caller supports us returning a tuplestore */
        if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
                ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("set-valued function called in context that cannot accept a set")));
        if (!(rsinfo->allowedModes & SFRM_Materialize))
                ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("materialize mode required, but it is not " \
                                                "allowed in this context")));

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
                elog(ERROR, "return type must be a row type");

        per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
        oldcontext = MemoryContextSwitchTo(per_query_ctx);

        if (tupstore == NULL)
            tupstore = tuplestore_begin_heap(true, false, work_mem);

        rsinfo->returnMode = SFRM_Materialize;
        rsinfo->setResult = tupstore;
        rsinfo->setDesc = tupdesc;

        MemoryContextSwitchTo(oldcontext);

        memset(values, 0, sizeof(values));
        memset(nulls, 0, sizeof(nulls));

        backends = pgstat_fetch_stat_numbackends();
        weight = ((backends * 1.0) / (MaxBackends)) * 100;

        if (xcdns_host != NULL)
            values[0] = CStringGetTextDatum(xcdns_host);
        else
            nulls[0] = true;

        values[1] = Int32GetDatum(weight);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);

        /* clean up and return the tuplestore */
        tuplestore_donestoring(tupstore);

        return (Datum) 0;
}




/*
 * Gather weights from local and remote coordinators
 */
static HTAB *gather_weights() {
        bool             found;
        EState           *estate;
        TupleTableSlot   *result;
        RemoteQuery      *step;
        RemoteQueryState *node;
        int              i, ncolumns;
        TupleDesc        tupledesc;
        MemoryContext    oldcontext;
        HTAB             *LocalHash;
        HASHCTL          ctl;
        HashKey          localkey;
        Entry            *localentry;

        char *query = "SELECT * FROM pgxc_dns_host_weight()";

        /* Build up RemoteQuery */
        step = makeNode(RemoteQuery);

        step->combine_type = COMBINE_TYPE_NONE;
        step->exec_nodes = makeNode(ExecNodes);
        step->sql_statement = query;
        step->force_autocommit = false;
        step->read_only = true;
        step->exec_type = EXEC_ON_COORDS;

        /* Build a local hash table to contain weights */
        memset(&ctl, 0, sizeof(ctl));

        ctl.keysize = sizeof(HashKey);
        ctl.entrysize = sizeof(Entry);
        ctl.hash = dns_hash_fn;
        ctl.match = dns_match_fn;

        LocalHash = hash_create("pgxc_dns local hash", 
                                MaxCoords,
                                &ctl,
                                HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
        if (!LocalHash)
                elog(ERROR, "out of memory");

        /* Build a tupdesc of all the OUT parameters */
        tupledesc = CreateTemplateTupleDesc(PGXC_DNS_HOST_WEIGHT_COLS, false);

        TupleDescInitEntry(tupledesc, (AttrNumber) 1, "host",
                              TEXTOID, -1, 0);

        TupleDescInitEntry(tupledesc, (AttrNumber) 2, "weight",
                              INT4OID, -1, 0);

        ncolumns = tupledesc->natts;

        for (i = 0; i < ncolumns; ++i)
        {
                Var         *var;
                TargetEntry *tle;

                var = makeVar(1,
                              tupledesc->attrs[i]->attnum,
                              tupledesc->attrs[i]->atttypid,
                              tupledesc->attrs[i]->atttypmod,
                              InvalidOid,
                              0);

                tle = makeTargetEntry((Expr *) var, tupledesc->attrs[i]->attnum, NULL, false);
                step->scan.plan.targetlist = lappend(step->scan.plan.targetlist, tle);
        }

        /* Execute query on the data nodes */
        estate = CreateExecutorState();

        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        estate->es_snapshot = GetActiveSnapshot();

        node = ExecInitRemoteQuery(step, estate, 0);
        MemoryContextSwitchTo(oldcontext);

        result = ExecRemoteQuery(node);
        while (result != NULL && !TupIsNull(result))
        {
                Datum   value;
                bool    isnull;
                HashKey key;
                Entry   *entry;
                char    *host;

                /* Process weights from the coordinator nodes */
                value = slot_getattr(result, 1, &isnull); /* host */
                if (isnull)
                        ereport(ERROR,
                                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                 errmsg("host must not be null")));

                host = TextDatumGetCString(value);

                /* Set up key for hashtable search */
                key.host_len = strlen(host);
                key.host_ptr = host;

                /* Find or create an entry with desired hash code */
                entry = (Entry *) hash_search(LocalHash, &key, HASH_ENTER, &found);
                if (!found) {
                        entry->key.host_ptr = entry->host;
                        entry->weight = 0;
                        memcpy(entry->host, key.host_ptr, key.host_len);
                        entry->host[key.host_len] = '\0';
                }

                value = slot_getattr(result, 2, &isnull); /* host */
                if (!isnull)
                        entry->weight = DatumGetInt32(value);

                /* fetch next */
                result = ExecRemoteQuery(node);
        }
        ExecEndRemoteQuery(node);

        /* Add the local information to the hashtable */
        localkey.host_len = strlen(xcdns_host);
        localkey.host_ptr = xcdns_host;

        localentry = (Entry *) hash_search(LocalHash, &localkey, HASH_ENTER, &found);
        if (!found) {
            localentry->key.host_ptr = localentry->host;
            localentry->weight = ((pgstat_fetch_stat_numbackends() * 1.0) / (MaxBackends)) * 100;
            memcpy(localentry->host, localkey.host_ptr, localkey.host_len);
            localentry->host[localkey.host_len] = '\0';
        }

        return LocalHash;
}


/*
 * Calculate hash value for a key
 */
static uint32 dns_hash_fn(const void *key, Size keysize) {
    const HashKey *k = (const HashKey *) key;

    return DatumGetUInt32(hash_any((const unsigned char *) k->host_ptr,
                                                           k->host_len));
}

/*
 * Compare two keys - zero means match
 */
static int dns_match_fn(const void *key1, const void *key2, Size keysize) {
    const HashKey *k1 = (const HashKey *) key1;
    const HashKey *k2 = (const HashKey *) key2;

    if (k1->host_len == k2->host_len &&
        memcmp(k1->host_ptr, k2->host_ptr, k1->host_len) == 0)
       return 0;
    else
       return 1;
}

