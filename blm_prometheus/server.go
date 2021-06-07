/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package main

/*
#include "hashmap.c"
typedef struct tablemap {
	char name[40];
} tablemap;
int user_compare(const void *a, const void *b,  void *udata) {
	const tablemap *ua = a;
	const tablemap *ub = b;
	return strncmp(a, b, sizeof(tablemap));
}
uint64_t user_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const tablemap *tm = item;
	return hashmap_sip(tm->name, sizeof(tm->name), seed0, seed1);
}
struct hashmap *init_hashmap() {
	return hashmap_new(sizeof(tablemap), 1000000, 0, 0, user_hash, user_compare, NULL);
}
void *hashmap_set_(struct hashmap *map, char *item) {
	return hashmap_set(map, item);
}
bool hashmap_get_(struct hashmap *map, char *key) {
	struct hashmap *ret = hashmap_get(map, key);
	return ret != NULL;
}
size_t hashmap_count_(struct hashmap *map) {
	return hashmap_count(map);
}
*/
//#cgo LDFLAGS: -Wl,--allow-multiple-definition
import "C"

import (
	"bufio"
	"bytes"
	"sync/atomic"
	// "container/list"
	"crypto/md5"
	"database/sql"
	"errors"
	"hash/adler32"
	"reflect"
	"unsafe"
	// "encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	// "sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	// "github.com/prometheus/common/model"
	_ "github.com/taosdata/driver-go/taosSql"
    "github.com/OneOfOne/xxhash"

	"github.com/prometheus/prometheus/prompb"
)

type Stats struct {
	HttpInCounter  int64
	HttpOutCounter int64

	SqlInCounter  int64
	SqlOutCounter int64

	SqlBatchCounter int64

	StableCounter int64
	TableCounter  int64

	StableGetCounter int64
	StableSetCounter int64

	TableGetCounter int64
	TableSetCounter int64
}

type SqlWorker struct {
	Counter int64
	Worker  chan string
}
type HttpWorker struct {
	Counter int64
	Worker  chan prompb.WriteRequest
}

type tablestruct struct {
	Fields    map[string]bool
	TableName string
	Tags      []string
	TagHash   string
}

// config
var (
	daemonIP       string
	daemonName     string
	sqlworkers     int
	httpworkers    int
	batchSize      int
	bufferSize     int
	dbname         string
	dbuser         string
	dbpassword     string
	rwport         string
	apiport        string
	debugprt       int
	taglen         int
	taglimit       int = 1024
	tagnumlimit    int
	stableMapCount uint32 = 1
	tableMapCount  uint32 = 64
	useCHashmap    bool   = false
	useHashmap     bool   = false
	bitmapSize     uint32
)

// Global vars
var (
	GlobalStats    Stats
	StartTime      time.Time
	SqlWorkers     []SqlWorker
	HttpWorkers    []HttpWorker
	sqlWg          sync.WaitGroup
	httpWg         sync.WaitGroup
	taosDriverName string = "taosSql"
	StableMap      []sync.Map
	TableMap       []sync.Map
	blmLog         *log.Logger
	tdurl          string
	logNameDefault string = "/var/log/taos/blm_prometheus.log"

	useBitmap    bool = false
	TableBitmap  []uint32
	bitmapLength uint

	chashmap *C.struct_hashmap
)

// Parse args:
func init() {
	StartTime = time.Now()
	flag.StringVar(&daemonIP, "tdengine-ip", "127.0.0.1", "TDengine host IP.")
	flag.StringVar(&daemonName, "tdengine-name", "", "TDengine host Name. in K8S, could be used to lookup TDengine's IP")
	flag.StringVar(&apiport, "tdengine-api-port", "6020", "TDengine restful API port")
	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&sqlworkers, "sql-workers", 1, "Number of parallel sql handler.")
	flag.IntVar(&httpworkers, "http-workers", 1, "Number of parallel http handler.")
	flag.StringVar(&dbname, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10203", "remote write port")
	flag.IntVar(&debugprt, "debugprt", 0, "if 0 not print, if 1 print the sql")
	flag.IntVar(&taglen, "tag-length", 128, "the max length of tag string,default is 30")
	flag.IntVar(&bufferSize, "buffer-size", 100, "the buffer size of metrics received")
	flag.IntVar(&tagnumlimit, "tag-num", 128, "the number of tags in a super table, default is 8")
	flag.BoolVar(&useCHashmap, "use-c-hashmap", false, "use c hashmap")
	flag.BoolVar(&useHashmap, "use-hashmap", false, "use go hashmap")

	flag.Parse()

	if daemonName != "" {
		s, _ := net.LookupIP(daemonName)
		daemonIP = fmt.Sprintf("%s", s[0])

		daemonIP = daemonIP + ":0"

		tdurl = daemonName
	} else {
		tdurl = daemonIP
		daemonIP = daemonIP + ":0"
	}

	logFile, err := os.OpenFile(logNameDefault, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	blmLog = log.New(logFile, "", log.LstdFlags)
	blmLog.SetPrefix("BLM_PRM")
	blmLog.SetFlags(log.LstdFlags | log.Lshortfile)
	blmLog.Printf("host: ip=%s port=%s dbname=%s", daemonIP, rwport, dbname)
}

func main() {
	createDatabase(dbname)

	initMap()
	go initHttpWorkers()
	go initSqlWorkers()
	go initAllStables()

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		idx := adler32.Checksum(compressed[:min(100, len(compressed)-1)])
		worker := &HttpWorkers[idx%uint32(httpworkers)]
		worker.Worker <- req
		atomic.AddInt64(&worker.Counter, 1)
		atomic.AddInt64(&GlobalStats.HttpInCounter, 1)
	})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		uptime := int64(time.Since(StartTime).Seconds())
		host := daemonIP + ":" + rwport
		fmts := []string{
			"# HELP blm_uptime blm_prometheus uptime",
			"# TYPE blm_uptime counter",
			"blm_uptime{host=\"%s\"} %d",
			"# HELP blm_http_workers http workers count",
			"# TYPE blm_http_workers gauge",
			"blm_http_workers{host=\"%s\"} %d",
			"# HELP blm_sql_workers sql workers count",
			"# TYPE blm_sql_workers gauge",
			"blm_sql_workers{host=\"%s\"} %d",
			"# HELP blm_http_in_counter http in counter",
			"# TYPE blm_http_in_counter counter",
			"blm_http_in_counter{host=\"%s\"} %d",
			"# HELP blm_http_out_counter http out counter",
			"# TYPE blm_http_out_counter counter",
			"blm_http_out_counter{host=\"%s\"} %d",
			"# HELP blm_sql_in_counter sql in counter",
			"# TYPE blm_sql_in_counter counter",
			"blm_sql_in_counter{host=\"%s\"} %d",
			"# HELP blm_sql_out_counter sql out counter",
			"# TYPE blm_sql_out_counter counter",
			"blm_sql_out_counter{host=\"%s\"} %d",
			"# HELP blm_sql_batch_counter sql batch counter",
			"# TYPE blm_sql_batch_counter counter",
			"blm_sql_batch_counter{host=\"%s\"} %d",
			"# HELP blm_table_count table counter",
			"# TYPE blm_table_count gauge",
			"blm_table_count{host=\"%s\"} %d",
			"# HELP blm_table_count_ex table counter",
			"# TYPE blm_table_count_ex gauge",
			"blm_table_count_ex{host=\"%s\"} %d",
			"# HELP blm_super_table_count super table counter",
			"# TYPE blm_super_table_count gauge",
			"blm_super_table_count{host=\"%s\"} %d",
			"# HELP blm_super_table_get_count super table get counter",
			"# TYPE blm_super_table_get_count counter",
			"blm_super_table_get_count{host=\"%s\"} %d",
			"# HELP blm_super_table_set_count super table set counter",
			"# TYPE blm_super_table_set_count counter",
			"blm_super_table_set_count{host=\"%s\"} %d",
			"# HELP blm_table_get_count super table get counter",
			"# TYPE blm_table_get_count counter",
			"blm_table_get_count{host=\"%s\"} %d",
			"# HELP blm_table_set_count super table set counter",
			"# TYPE blm_table_set_count counter",
			"blm_table_set_count{host=\"%s\"} %d",
		}
		tableCount := 0
		if useCHashmap {
			tableCount = int(C.hashmap_count_(chashmap))
		}
		s := fmt.Sprintf(strings.Join(fmts, "\n"),
			host, uptime, host, httpworkers, host, sqlworkers,
			host, GlobalStats.HttpInCounter, host, GlobalStats.HttpOutCounter,
			host, GlobalStats.SqlInCounter, host, GlobalStats.SqlOutCounter,
			host, GlobalStats.SqlBatchCounter,
			host, GlobalStats.TableCounter, host, tableCount,
			host, GlobalStats.StableCounter,
			host, GlobalStats.StableGetCounter, host, GlobalStats.StableSetCounter,
			host, GlobalStats.TableGetCounter, host, GlobalStats.TableSetCounter,
		)
		w.Write(String2Bytes(s))
	})
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {

		/*
			compressed, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// var output string = ""
				schema, ok := StableMap.Load(string(compressed))
				if !ok {
					// output = "the stable is not created!"
				} else {
						ntag := schema.(nametag)
						tbtaglist := ntag.taglist
						tbtagmap := ntag.tagmap
						//annotlen := ntag.annotlen
						output = "tags: "
						for e := tbtaglist.Front(); e != nil; e = e.Next() {
							output = output + e.Value.(string) + " | "
						}
						output = output + "\ntagmap: "
						s := fmt.Sprintln(tbtagmap)
						output = output + s
				}
		*/

		/*
			res := queryTableStruct2(string(compressed))
			output = output + "\nTable structure:\n" + res
			s := fmt.Sprintf("query result:\n %s\n", output)
			//blmLog.Println(s)
		*/
		s := ""
		w.Write(String2Bytes(s))
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	if debugprt == 5 {
		go TestSerialization()
	}
	blmLog.Fatal(http.ListenAndServe(":"+rwport, nil))
}

func initMap() {
	StableMap = make([]sync.Map, stableMapCount)
	if useCHashmap {
		chashmap = C.init_hashmap()
	} else if useHashmap {

	} else {
		TableMap = make([]sync.Map, tableMapCount)
	}
}

func initSqlWorkers() {
	for i := 0; i < sqlworkers; i++ {
		SqlWorkers = append(SqlWorkers, SqlWorker{Counter: 0, Worker: make(chan string, batchSize)})
	}

	for i := 0; i < sqlworkers; i++ {
		sqlWg.Add(1)
		go processSqls(i)
	}

	sqlWg.Wait()
}

func initHttpWorkers() {
	for i := 0; i < httpworkers; i++ {
		HttpWorkers = append(HttpWorkers, HttpWorker{Counter: 0, Worker: make(chan prompb.WriteRequest, bufferSize)})
	}

	for i := 0; i < httpworkers; i++ {
		httpWg.Add(1)
		go processHttps(i)
	}

	httpWg.Wait()
}

func initAllStables() error {
	db, err := getDb()
	if err != nil {
		blmLog.Printf("Get db instance for initAllStables failed: %s", err)
		return err
	}
	defer db.Close()

	blmLog.Printf("Begin to load all stables ...")
	rows, err := db.Query("show stables")
	if err != nil {
		blmLog.Printf("Show stables failed: %s", err)
		return err
	}
	defer rows.Close()

	var name string
	var createdTime string
	var columns int
	var tags int
	var tables int
	var succcnt int = 0
	var totalcnt int = 0
	for rows.Next() {
		totalcnt++
		err := rows.Scan(&name, &createdTime, &columns, &tags, &tables)
		if err != nil {
			blmLog.Printf("Scan rows for initAllStables failed: %s", err)
			continue
		}
		stableInfo, err := queryTableStruct2(name, db)
		if err != nil {
			blmLog.Printf("Query table struct for %s in initAllStables failed: %s", name, err)
			continue
		}
		slot := blmChecksum(name) % stableMapCount
		stableMapSet(name, stableInfo, true, slot)
		succcnt++
	}
	blmLog.Printf("Load all stables done: total=%d, succ=%d", totalcnt, succcnt)
	return nil
}

func processHttps(index int) error {
	db, err := getDb()
	if err != nil {
		blmLog.Printf("Get db instance for processHttps failed: %s", err)
		return err
	}
	defer db.Close()

	worker := &HttpWorkers[index]
	for req := range worker.Worker {
		atomic.AddInt64(&GlobalStats.HttpOutCounter, 1)
		uts := time.Now().Unix()
		for _, ts := range req.Timeseries {
			checkStable(&ts, db, uts)
		}
	}
	httpWg.Done()
	return nil
}

func processSqls(index int) error {
	db, err := getDb()
	if err != nil {
		blmLog.Printf("Get db instance for processSqls failed: %s", err)
		return err
	}
	defer db.Close()

	var sqls strings.Builder
	sqls.WriteString("INSERT INTO ")

	counter := 0
	worker := &SqlWorkers[index]
	for sql := range worker.Worker {
		sqls.WriteString(sql)
		atomic.AddInt64(&GlobalStats.SqlOutCounter, 1)

		counter++
		if counter >= batchSize {
			atomic.AddInt64(&GlobalStats.SqlBatchCounter, 1)
			sql_ := sqls.String()
			_, err := execSql2(sql_, db)
			if err != nil {
				blmLog.Printf("Run sql %s failed: %s", sql_, err)
			}
			counter = 0
			sqls.Reset()
			sqls.WriteString("INSERT INTO ")
		}
	}
	if counter > 0 {
		atomic.AddInt64(&GlobalStats.SqlBatchCounter, 1)
		sql_ := sqls.String()
		_, err := execSql2(sql_, db)
		if err != nil {
			blmLog.Printf("Run sql %s failed: %s", sql_, err)
		}
		counter = 0
		sqls.Reset()
	}
	sqlWg.Done()
	return nil
}

func getDb() (*sql.DB, error) {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/"+dbname)
	if err != nil {
		blmLog.Printf("Connect to db failed: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				db, err = sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/"+dbname)
				count--
			} else {
				if err != nil {
					blmLog.Printf("Connect to db failed after 5 tries: %s\n", err)
					return db, err
				}
				break
			}
		}
	}
	return db, err
}

func checkTableExists(tbname string, db *sql.DB) (bool, error) {
	rows, err := db.Query("DESCRIBE " + tbname)
	if err != nil {
		blmLog.Printf("Describe %s failed, return %s", tbname, err)
		return false, err
	}
	defer rows.Close()
	return true, nil
}

func queryTableStruct2(tbname string, db *sql.DB) (tablestruct, error) {
	var ts tablestruct
	ts.Fields = make(map[string]bool)

	rows, err := db.Query("DESCRIBE " + tbname)
	if err != nil {
		blmLog.Printf("Describe %s failed, return %s", tbname, err)
		return ts, err
	}
	defer rows.Close()

	len := 0
	note := ""
	field := ""
	type_ := ""

	for rows.Next() {
		err := rows.Scan(&field, &type_, &len, &note)
		if err == nil && note == "TAG" && field != "taghash" {
			ts.Fields[field] = true
			ts.Tags = append(ts.Tags, field)
		}
	}

	ts.TagHash = md5V2(strings.Join(ts.Tags, "|"))
	ts.TableName = tbname
	return ts, nil
}

func addField(table string, field string, db *sql.DB) (sql.Result, error) {
	sqlcmd := fmt.Sprintf("ALTER TABLE %s ADD TAG %s BINARY(%d)\n", table, field, taglen)
	return execSql2(sqlcmd, db)
}

func checkStable(ts *prompb.TimeSeries, db *sql.DB, uts int64) error {
	var err error
	stableName := ""
	tagMap := make(map[string]*string)

	for _, l := range ts.Labels {
		name := strings.ToLower(l.Name)
		if name == "__name__" {
			stableName = l.Value
			continue
		} else {
			tagMap["t_"+name] = &l.Value
		}
	}
	if stableName == "" {
		return errors.New("can not find metric name")
	}

	stableName = escapeTableName(stableName)
	sslot := blmChecksum(stableName) % stableMapCount

	var stableInfo tablestruct
	var valid bool = false
	stableInfo_, ok := stableMapGet(stableName, sslot)
	if ok {
		stableInfo = stableInfo_.(tablestruct)
	} else {
		stableInfo, err = queryTableStruct2(stableName, db)
		if err == nil {
			stableMapSet(stableName, stableInfo, true, sslot)
		} else {
			var sql strings.Builder
			sql.WriteString("CREATE TABLE IF NOT EXISTS ")
			sql.WriteString(stableName)
			sql.WriteString("(ts TIMESTAMP, value DOUBLE) TAGS (taghash BINARY(34)")
			for tag, _ := range tagMap {
				sql.WriteString(fmt.Sprintf(",%s BINARY(%d)", tag, taglen))
			}
			sql.WriteString(")\n")
			sql_ := sql.String()
			_, err := execSql2(sql_, db)
			if err == nil {
				stableInfo, err = queryTableStruct2(stableName, db)
				if err == nil {
					stableMapSet(stableName, stableInfo, true, sslot)
				} else {
					blmLog.Printf("Query table struct for %s after create failed: %s", stableName, err)
					return err
				}
			} else {
				blmLog.Printf("Create table %s [%s] failed: %s", stableName, sql_, err)
				return err
			}
			valid = true
		}
	}
	if !valid {
		changed := false
		fields := stableInfo.Fields
		for tag, _ := range tagMap {
			_, exists := fields[tag]
			if !exists {
				_, err := addField(stableName, tag, db)
				errstr := fmt.Sprintf("%s", err)
				if !strings.Contains(errstr, "duplicated column names") {
					changed = true
				}
			}
		}
		if changed {
			stableInfo, err = queryTableStruct2(stableName, db)
			if err != nil {
				blmLog.Printf("Query table struct %s after add tag failed: %s", stableName, err)
				return err
			} else {
				stableMapSet(stableName, stableInfo, false, sslot)
			}
		}
		valid = true
	}

	tableHash := generateTableHash(stableName, &stableInfo.Tags, &tagMap)
	slot := blmChecksum(tableHash) % tableMapCount
	_, exists := tableMapGet(tableHash, slot, uts)
	if !exists {

		_, err = queryTableStruct2(tableHash, db)
		if err != nil {
			var sql bytes.Buffer
			sql.WriteString("CREATE TABLE IF NOT EXISTS ")
			sql.WriteString(tableHash)
			sql.WriteString(" USING ")
			sql.WriteString(stableName)
			sql.WriteString(" TAGS(\"")
			sql.WriteString(stableInfo.TagHash)
			sql.WriteString("\"")
			for _, tag := range stableInfo.Tags {
				value, exists := tagMap[tag]
				if exists {
					sql.WriteString(", \"" + (*value)[:min(128, len(*value))] + "\"")
				} else {
					sql.WriteString(", null")
				}
			}
			sql.WriteString(")")
			sql_ := sql.String()
			_, err := execSql2(sql_, db)
			if err == nil {
				tableMapSet(tableHash, true, slot, uts)
			} else {
				blmLog.Printf("try to create table %s [%s] failed: %s", tableHash, sql_, err)
				return err
			}
		} else {
			tableMapSet(tableHash, true, slot, uts)
		}
	}
	serializeTDengine2(ts, tableHash)
	return nil
}

func stableMapGet(key string, slot uint32) (interface{}, bool) {
	atomic.AddInt64(&GlobalStats.StableGetCounter, 1)
	return StableMap[slot].Load(key)
}

func stableMapSet(key string, value interface{}, inc bool, slot uint32) {
	StableMap[slot].Store(key, value)
	if inc {
		atomic.AddInt64(&GlobalStats.StableCounter, 1)
	}
	atomic.AddInt64(&GlobalStats.StableSetCounter, 1)
}

func tableMapGet(key string, slot uint32, uts int64) (interface{}, bool) {
	atomic.AddInt64(&GlobalStats.TableGetCounter, 1)
	if useCHashmap {
		cs := C.CString(key)
		defer C.free(unsafe.Pointer(cs))
		exists := C.hashmap_get_(chashmap, cs)
		if (debugprt & 4) == 4 {
			blmLog.Printf("tableMapGet: key=%s exists=%d", key, bool(exists))
		}
		return nil, bool(exists)
	} else if useBitmap {
		checksum := blmChecksum(key)
		idx := uint32(checksum % uint32(bitmapSize))
		bitmapIdx := idx >> 2
		bits := (idx & 3) << 3
		value := atomic.LoadUint32(&TableBitmap[bitmapIdx])
		weeks := (value >> bits) & 0xff
		curweeks := getCurrentWeeks(uts, checksum)
		if (debugprt & 2) == 2 {
			blmLog.Printf("tableMapGet: key=%s idx=%d bitmapIdx=%d value=%d weeks=%d curweeks=%d",
				key, idx, bitmapIdx, value, weeks, curweeks)
		}
		return nil, weeks == curweeks
	}
	return TableMap[slot].Load(key)
}
func tableMapSet(key string, value interface{}, slot uint32, uts int64) {
	if useCHashmap {
		cs := C.CString(key)
		defer C.free(unsafe.Pointer(cs))
		C.hashmap_set_(chashmap, cs)
	} else if useBitmap {
		checksum := blmChecksum(key)
		idx := uint32(checksum % uint32(bitmapSize))
		bitmapIdx := idx >> 2
		bits := (idx & 3) << 3
		curweeks := getCurrentWeeks(uts, checksum)
		value := curweeks << bits

		originValue := atomic.LoadUint32(&TableBitmap[bitmapIdx])
		if (originValue & value) != value {
			if (debugprt & 2) == 2 {
				blmLog.Printf("tableMapSet: key=%s idx=%d bitmapIdx=%d value=%d originValue=%d curweeks=%d",
					key, idx, bitmapIdx, value, originValue, curweeks)
			}
			swapped := atomic.CompareAndSwapUint32(&TableBitmap[bitmapIdx], originValue, originValue|value)
			// try again
			if !swapped {
				originValue = atomic.LoadUint32(&TableBitmap[bitmapIdx])
				if (originValue & value) != value {
					atomic.CompareAndSwapUint32(&TableBitmap[bitmapIdx], originValue, originValue|value)
				}
			}
		}
	} else {
		TableMap[slot].Store(key, value)
	}
	atomic.AddInt64(&GlobalStats.TableCounter, 1)
	atomic.AddInt64(&GlobalStats.TableSetCounter, 1)
}

// not really weeks, just steal the concept
func getCurrentWeeks(uts int64, checksum uint32) uint32 {
	weeks := ((uts + int64(checksum&0x7ffff)) >> 19) & 0x7f
	return uint32(weeks) + 1
}

func generateTableHash(stableName string, tags *[]string, tagMap *map[string]*string) string {
	var tm = *tagMap
	var origin strings.Builder
	origin.WriteString(stableName)
	for _, tag := range *tags {
		origin.WriteString("|")
		value, exists := tm[tag]
		if exists {
			origin.WriteString(*value)
		}
	}
	return "md5_" + md5V2(origin.String())
}

func escapeTableName(s string) string {
	len_ := len(s)
	if len_ == 0 {
		return s
	}

	var bb bytes.Buffer
	bb.WriteString(s[:min(190, len_)])
	bb_ := bb.Bytes()

	for b := range ":.-" {
		i := 0
		for i < len_ {
			pos := bytes.IndexByte(bb_[i:], byte(b))
			if pos < 0 {
				break
			}
			bb_[pos] = '_'
			i = pos + 1
		}
	}
	return bb.String()
}

func serializeTDengine2(m *prompb.TimeSeries, tbn string) error {
	idx := blmChecksum(tbn)
	vl := m.Samples[0].GetValue()
	vls := strconv.FormatFloat(vl, 'E', -1, 64)

	if vls == "NaN" {
		vls = "null"
	}
	tl := m.Samples[0].GetTimestamp()
	tls := strconv.FormatInt(tl, 10)
	sqlcmd := " " + tbn + " VALUES(" + tls + "," + vls + ")\n"
	sqlworker := &SqlWorkers[idx%uint32(sqlworkers)]
	sqlworker.Worker <- sqlcmd
	atomic.AddInt64(&sqlworker.Counter, 1)
	atomic.AddInt64(&GlobalStats.SqlInCounter, 1)
	return nil
}

func createDatabase(dbname string) error {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	sqlcmd := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ", dbname)
	_, err = db.Exec(sqlcmd)
	if err != nil {
		blmLog.Printf("Create database %s [%s] failed: %s", dbname, sqlcmd, err)
	}
	return err
}

func execSql2(sqlcmd string, db *sql.DB) (sql.Result, error) {
	res, err := db.Exec(sqlcmd)
	if (debugprt & 1) == 1 {
		fmt.Println("XXX TRY TO exec [", sqlcmd, "] return [", err, "]")
	}
	/*
		if err != nil {
			var count int = 2
			for {
				if err != nil && count > 0 {
					<-time.After(time.Second * 1)
					res, err = db.Exec(sqlcmd)
					count--
				} else {
					break
				}
			}
		}
	*/
	return res, err
}

func md5V2(str string) string {
	data := String2Bytes(str)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	return md5str
}

func TestSerialization() {
	var req prompb.WriteRequest
	var ts []prompb.TimeSeries
	var tse prompb.TimeSeries
	var sample *prompb.Sample
	var label prompb.Label
	var lbs []prompb.Label
	promPath, err := os.Getwd()
	if err != nil {
		fmt.Printf("can't get current dir :%s \n", err)
		os.Exit(1)
	}
	promPath = filepath.Join(promPath, "testData/blm_prometheus.log")
	testfile, err := os.OpenFile(promPath, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer testfile.Close()
	fmt.Println(promPath)
	buf := bufio.NewReader(testfile)
	total := 0
	lasttime := "20:40:20"
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok! line:", total)
				break
			} else {
				fmt.Println("Read file error!", err)
				return
			}
		}
		total++
		sa := strings.Split(line, " ")

		if strings.HasPrefix(line, "201:") {
			if sa[3] != lasttime {
				idx := blmChecksum(line)
				worker := &HttpWorkers[idx%uint32(httpworkers)]
				worker.Worker <- req
				atomic.AddInt64(&worker.Counter, 1)
				atomic.AddInt64(&GlobalStats.HttpInCounter, 1)

				lasttime = sa[3]
				req.Timeseries = req.Timeseries[:0]
				ts = ts[:0]
			}
			tse.Samples = make([]prompb.Sample, 0)
			T, _ := strconv.ParseInt(sa[3][:(len(sa[3])-1)], 10, 64)
			V, _ := strconv.ParseFloat(sa[5][:(len(sa[5])-1)], 64)
			sample = &prompb.Sample{
				Value:     V,
				Timestamp: T,
			}
			tse.Samples = append(tse.Samples, *sample)
		} else if strings.HasPrefix(line, "202:") {
			lbs = make([]prompb.Label, 0)
			lb := strings.Split(line[5:], "{")
			label.Name = "__name__"
			label.Value = lb[0]
			lbs = append(lbs, label)
			lbc := strings.Split(lb[1][:len(lb[1])-1], ", ")
			for i := 0; i < len(lbc); i++ {
				content := strings.Split(lbc[i], "=\"")
				label.Name = content[0]
				if i == len(lbc)-1 {
					label.Value = content[1][:len(content[1])-2]
				} else {

					label.Value = content[1][:len(content[1])-1]
				}
				lbs = append(lbs, label)
			}
			tse.Labels = lbs
			ts = append(ts, tse)
			req.Timeseries = ts
		}

	}

}
func blmChecksum(str string) uint64 {
    return xxhash.Checksum64(String2Bytes(str))
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func String2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
