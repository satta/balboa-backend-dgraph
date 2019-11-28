package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/DCSO/balboa/observation"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

func (d *DgraphHandler) updateSensorIDCache(chunk []*observation.InputObservation) {
	d.SensorIDCacheUpdateLock.Lock()
	defer d.SensorIDCacheUpdateLock.Unlock()

	txn := d.Client.NewTxn()
	ctx := context.Background()

	var r CheckRes
	toCheck := make(map[string]struct{})
	var dummy struct{}
	for _, o := range chunk {
		if _, ok := d.SensorIDCache.Get(o.SensorID); !ok {
			toCheck[o.SensorID] = dummy
		}
	}
	if len(toCheck) == 0 {
		return
	}

	var buf bytes.Buffer
	for k := range toCheck {
		buf.WriteString(k)
		buf.WriteString(" ")
	}
	q := `
	query getSensors($sids: string){
		ResultsSensors(func: anyofterms(sensor_id, $sids)) {
			uid
			sensor_id
		}
	}`
	res, err := txn.QueryWithVars(ctx, q, map[string]string{"$sids": buf.String()})
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(res.GetJson(), &r)
	if err != nil {
		log.Fatal(err)
	}
	foundSensors := make(map[string]string)
	for _, v := range r.ResultsSensors {
		foundSensors[v.SensorID] = v.UID
	}

	for k := range toCheck {
		if id, ok := foundSensors[k]; ok {
			log.Debugf("found sensor %s in DB", k)
			d.SensorIDCache.Set(k, []byte(id), cache.DefaultExpiration)
		} else {
			log.Debugf("sensor %s not found in DB ", k)
			mutString := fmt.Sprintf(`_:sensor <sensor_id> "%s" .`, k)
			log.Debugf("doing: %s", mutString)
			mu := &api.Mutation{
				SetNquads: []byte(mutString),
				CommitNow: true,
			}
			res, err := d.Client.NewTxn().Mutate(ctx, mu)
			if err != nil {
				log.Fatal(err)
			}
			d.SensorIDCache.Set(k, res.Uids["sensor"], cache.DefaultExpiration)
		}
	}
}
