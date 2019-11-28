package handler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/DCSO/balboa/db"
	"github.com/DCSO/balboa/observation"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

const (
	maxChunk   = 500
	timeFormat = "2006-01-02T15:04:05Z"
)

// CheckRes is a struct to hold values looked up during import
type CheckRes struct {
	ResultsSensors []struct {
		UID      string `json:"uid"`
		SensorID string `json:"sensor_id"`
	}
	ResultsObs []struct {
		UID        string                   `json:"uid"`
		Ohash      string                   `json:"ohash"`
		SeenBySame []map[string]interface{} `json:"seen_by_same"`
	}
}

// GetRes is a struct holding values to be returned from a query
type GetRes struct {
	ResultsObs []struct {
		Rrname string                   `json:"rrname"`
		Rdata  string                   `json:"rdata"`
		Rrtype string                   `json:"rrtype"`
		SeenBy []map[string]interface{} `json:"seen_by"`
	}
}

// DgraphHandler is a Handler for balboa implemented to communicate
// with the Dgraph database
type DgraphHandler struct {
	Client                  *dgo.Dgraph
	BufChunks               []*observation.InputObservation
	SensorIDCacheUpdateLock sync.Mutex
	SensorIDCache           *cache.Cache
}

// CheckAndSetSchema ensures that a schema is set on the database that
// is used in the handler.
func (d *DgraphHandler) CheckAndSetSchema() {
	ctx := context.Background()
	//TODO: actually check

	op := &api.Operation{}
	op.Schema = `
	type Observation {
		ohash: string
		rrname: string
		rdata: int
		rrtype: string
		seen_by: [Sensor]
	}
	
	type Sensor {
		sensor_id: string
	}
	
	ohash: string @index(term, hash) @upsert .
	rrname: string @index(term, trigram) .
	rdata: string @index(term, trigram) .
	rrtype: string @index(term) .
	sensor_id: string @index(term, hash) .
	seen_by: [uid] @reverse .
	`
	err := d.Client.Alter(ctx, op)
	if err != nil {
		log.Fatal(err)
	}
}

func (d *DgraphHandler) handleChunk(chunk []*observation.InputObservation) {
	startTime := time.Now()
	d.updateSensorIDCache(chunk)

	txn := d.Client.NewTxn()
	ctx := context.Background()

	// query for observations by their
	i := 0
	ohashesIn := make([]string, maxChunk)
	for _, obs := range chunk {
		oKey := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s%s", obs.Rrname, obs.Rdata, obs.Rrtype))))
		ohashesIn[i] = oKey
		i++
	}
	q := `query checkObs($oids: string){
		ResultsObs(func: anyofterms(ohash, $oids)) {
			uid
			ohash
			seen_by_same: seen_by @facets(count, first_seen) {
				sensor_id
			}
		}
	}`
	res, err := txn.QueryWithVars(ctx, q, map[string]string{"$oids": strings.Join(ohashesIn, " ")})
	if err != nil {
		log.Fatal(err)
	}
	var r CheckRes
	err = json.Unmarshal(res.Json, &r)
	if err != nil {
		log.Fatal(err)
	}
	found := make(map[string]struct {
		uid    string
		seenby []map[string]interface{}
	})
	for _, f := range r.ResultsObs {
		found[f.Ohash] = struct {
			uid    string
			seenby []map[string]interface{}
		}{
			uid:    f.UID,
			seenby: f.SeenBySame,
		}
	}

	i = 0
	mutNquad := make([]string, 0)
	for _, obs := range chunk {
		var sensorUID string
		if suid, ok := d.SensorIDCache.Get(obs.SensorID); !ok {
			mutNquad = append(mutNquad, fmt.Sprintf(`_:sensor%s <sensor_id> "%s" .`,
				obs.SensorID, obs.SensorID))
			sensorUID = fmt.Sprintf(`_:sensor%s`, obs.SensorID)
		} else {
			sensorUID = fmt.Sprintf("<%s>", suid)
		}
		cntValue := uint64(0)
		firstSeenString := obs.TimestampStart.Format(timeFormat)
		ohash := ohashesIn[i]
		i++
		obsUID := fmt.Sprintf("_:obs%s", ohash)
		if vals, ok := found[ohash]; !ok {
			mutNquad = append(mutNquad, fmt.Sprintf(`%s <rrname> "%s" .`, obsUID, obs.Rrname))
			mutNquad = append(mutNquad, fmt.Sprintf(`%s <rrtype> "%s" .`, obsUID, obs.Rrtype))
			mutNquad = append(mutNquad, fmt.Sprintf(`%s <rdata> "%s" .`, obsUID, obs.Rdata))
			mutNquad = append(mutNquad, fmt.Sprintf(`%s <ohash> "%s" .`, obsUID, ohash))
			cntValue = uint64(obs.Count)
		} else {
			obsUID = fmt.Sprintf("<%s>", vals.uid)
			if len(vals.seenby) == 0 {
				cntValue = uint64(obs.Count)
			} else {
				// TODO: avoid O(|sensors|)
				for _, s := range vals.seenby {
					if sid, ok := s["sensor_id"]; ok {
						if sid == obs.SensorID {
							cntValue = uint64(s["seen_by_same|count"].(float64)) + uint64(obs.Count)
							firstSeenString = s["seen_by_same|first_seen"].(string)
							break
						}
					}
				}
				if cntValue == 0 {
					cntValue = uint64(obs.Count)
				}
			}
		}
		lastSeenString := obs.TimestampEnd.Format(timeFormat)
		mutNquad = append(mutNquad, fmt.Sprintf(`%s <seen_by> %s (count=%d,last_seen=%s,first_seen=%s) .`,
			obsUID, sensorUID, cntValue, lastSeenString, firstSeenString))
	}

	mutStr := strings.Join(mutNquad, "\n")
	mu := &api.Mutation{
		SetNquads: []byte(mutStr),
	}
	res, err = txn.Mutate(ctx, mu)
	if err != nil {
		log.Fatal(err)
	}
	txn.Commit(ctx)

	log.Debugf("chunk done, took %v", time.Since(startTime))
}

// HandleObservations processes a single observation for insertion.
func (d *DgraphHandler) HandleObservations(obs *observation.InputObservation) {
	if len(d.BufChunks) < maxChunk {
		d.BufChunks = append(d.BufChunks, obs)
	} else {
		log.Debug("enqueuing chunk")
		go d.handleChunk(d.BufChunks)
		d.BufChunks = make([]*observation.InputObservation, 0)
	}
}

// HandleQuery implements a query for a combination of parameters.
func (d *DgraphHandler) HandleQuery(qr *db.QueryRequest, conn net.Conn) {
	txn := d.Client.NewReadOnlyTxn()
	ctx := context.Background()
	sfunc := ""
	seenFilter := ""
	paramMap := make(map[string]string)
	filters := make(map[string]string)
	requestedVars := make(map[string]string)

	if qr.Hrdata {
		if qr.Hrrname {
			requestedVars["$rrname"] = "string"
			requestedVars["$rdata"] = "string"
			sfunc = "(func: eq(rrname, $rrname))"
			paramMap["$rrname"] = qr.Qrrname
			paramMap["$rdata"] = qr.Qrdata
			filters["rdata"] = "$rdata"
		} else {
			requestedVars["$rdata"] = "string"
			sfunc = "(func: eq(rdata, $rdata))"
			paramMap["$rdata"] = qr.Qrdata
		}
	} else {
		if qr.Hrrname {
			requestedVars["$rrname"] = "string"
			sfunc = "(func: eq(rrname, $rrname))"
			paramMap["$rrname"] = qr.Qrrname
		} else {
			log.Warn("encountered empty request, ignoring")
			return
		}
	}
	if qr.HsensorID {
		seenFilter = fmt.Sprintf("@filter(eq(sensor_id, \"%s\"))", qr.QsensorID)
	}
	if qr.Hrrtype {
		requestedVars["$rrtype"] = "string"
		filters["rrtype"] = "$rrtype"
		paramMap["$rrtype"] = qr.Qrrtype
	}

	// construct query parameters
	qParams := ""
	i := 1
	for k, v := range requestedVars {
		qParams += fmt.Sprintf("%s:%s", k, v)
		if i != len(requestedVars) {
			qParams += ","
		}
		i++
	}
	// construct filters
	i = 1
	if len(filters) > 0 {
		sfunc += " @filter("
		for k, v := range filters {
			sfunc += fmt.Sprintf("eq(%s, %s)", k, v)
			if i != len(filters) {
				sfunc += " AND "
			}
			i++
		}
		sfunc += ")"
	}

	// populate query
	q := `query getObs(%s) {
		ResultsObs%s {
			rrname
			rdata
			rrtype
			seen_by @facets(count, first_seen, last_seen) %s{
				sensor_id
			}
		}
	}`
	q = fmt.Sprintf(q, qParams, sfunc, seenFilter)
	res, err := txn.QueryWithVars(ctx, q, paramMap)
	if err != nil {
		log.Fatal(err)
	}
	var r GetRes
	err = json.Unmarshal(res.Json, &r)
	if err != nil {
		log.Fatal(err)
	}

	enc := db.MakeEncoder()
	startRes, err := enc.EncodeQueryStreamStartResponse()
	if err != nil {
		log.Error(err)
	}
	conn.Write(startRes.Bytes())
	for _, v := range r.ResultsObs {
		for _, s := range v.SeenBy {
			fs, err := time.Parse(timeFormat, s["seen_by|first_seen"].(string))
			if err != nil {
				log.Error(err)
			}
			ls, err := time.Parse(timeFormat, s["seen_by|last_seen"].(string))
			if err != nil {
				log.Error(err)
			}
			dRes, err := enc.EncodeQueryStreamDataResponse(observation.Observation{
				RRName:    v.Rrname,
				RData:     v.Rdata,
				RRType:    v.Rrtype,
				SensorID:  s["sensor_id"].(string),
				FirstSeen: fs,
				LastSeen:  ls,
				Count:     uint(s["seen_by|count"].(float64)),
			})
			if err != nil {
				log.Error(err)
			}
			conn.Write(dRes.Bytes())
		}
	}
	endRes, err := enc.EncodeQueryStreamEndResponse()
	if err != nil {
		log.Error(err)
	}
	conn.Write(endRes.Bytes())
}

// HandleDump is not implemented yet.
func (d *DgraphHandler) HandleDump(dr *db.DumpRequest, conn net.Conn) {
	enc := db.MakeEncoder()
	errRes, err := enc.EncodeErrorResponse(db.ErrorResponse{
		Message: fmt.Sprintf("dump to %s not supported yet", dr.Path),
	})
	if err != nil {
		log.Error(err)
	}
	conn.Write(errRes.Bytes())
}

// HandleBackup is not implemented yet.
func (d *DgraphHandler) HandleBackup(br *db.BackupRequest) {
	log.Error("backup not supported yet")
}
