package dfstore

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	
	"github.com/bobbae/q"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-redis/redis"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	//"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	_ "github.com/lib/pq"

)

type DFStore struct {
	Kind           string
	Name           string
	Host           string
	Port           string
	User           string
	Password       string
	URL            string
	Path           string
	Q              string
	DBName         string
	TableName      string
	Ctx           context.Context
	RedisClient    *redis.Client
	PostgresClient *sql.DB
	MySQLClient    *sql.DB
	//SQLiteClient *sqlite.Client
	MongodbClient *mongo.Client
	//ElasticClient *elastic.Client
	TimescaleClient *sql.DB
}

func New(ctx context.Context, kind string) (*DFStore, error) {
	dfs := DFStore{}
	dfs.Ctx = ctx
	var URL string
	if !strings.Contains(kind, ":")  {
		switch kind {
		case "default":
			URL = "postgres://pguser:password@localhost:5432/dfstore1/table1?sslmode=disable"
		case "document":
			URL = "mongo://root:rootpass@localhost:27017/dfstore1/table1?maxPoolSize=20&w=majority"
		case "timeseries":
			URL = "timescale://tsuser:password@localhost:5432/dfstore1/table1?sslmode=disable"
		case "memory":
			URL = "redis://root:password@localhost:6379/0/table1"
		case "blob":
			URL = "blob://root:password@/home/testuser/blobdir:0/dfstore1/table1"
			// URL = "blob://root:password@aws.bucket:0/dfstore1/table1"

		default:
			URL = kind
		}
	}
	
	u, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}
	q.Q(u)
	dfs.Kind = u.Scheme
	dfs.URL = URL
	dfs.Host, dfs.Port, err = net.SplitHostPort(u.Host)
	if err != nil {
		return nil, err
	}
	dfs.Password, _  = u.User.Password()
	dfs.User = u.User.Username()
	dfs.Path = u.Path
	dfs.DBName, dfs.TableName = filepath.Split(dfs.Path)
	dfs.DBName = strings.Replace(dfs.DBName, "/", "", -1)
	dfs.Q = u.RawQuery
	q.O = "stderr"
	q.P = ".*"
	q.Q(dfs)
	switch dfs.Kind {
	case "redis":
		DBNum, err := strconv.Atoi(dfs.DBName)
		if err != nil {
			return nil, err
		}
		rclient := redis.NewClient(&redis.Options{
			Addr:     dfs.Host + ":" + dfs.Port,
			Password: dfs.Password,
			DB:       DBNum,
		})
		//defer rclient.Close()
		if err := rclient.Ping().Err(); err != nil {
			return nil, err
		}
		dfs.RedisClient = rclient
	case "postgres":
		psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
			"password=%s dbname=%s sslmode=disable",
			dfs.Host, dfs.Port, dfs.User, dfs.Password, dfs.DBName)
		pdb, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			return nil, err
		}
		//defer pdb.Close()
		if err := pdb.Ping(); err != nil {
			return nil, err
		}
		//pdb.Query("CREATE database DBName")
		//pdb.Query("USE DBName")
		dfs.PostgresClient = pdb
	case "mongo":
		mongo_client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dfs.URL))
		if err != nil {
			return nil, err
		}
		if err := mongo_client.Ping(context.TODO(), readpref.Primary()); err != nil {
			return nil, err
		}
		dfs.MongodbClient = mongo_client
	default:
		return nil,fmt.Errorf("not supported: %v", dfs.Kind)
	}

	return &dfs, nil
}

func (dfs DFStore) Close() error {
	switch dfs.Kind {
	case "redis":
		if err := dfs.RedisClient.Close(); err != nil {
			return err
		}
	case "postgres":
		if err := dfs.PostgresClient.Close(); err != nil {
			return err
		}
	case "mongo":
		if err := dfs.MongodbClient.Disconnect(context.TODO()); err != nil {
			return err
		}
	default:
		return fmt.Errorf("not supported: %v", dfs.Kind)
	}
	return nil
}

func (dfs DFStore) WriteRecords(dataRows [][]string) error {
	switch dfs.Kind {
	case "redis":
		if err := dfs.RedisWriteRecords(dataRows); err != nil {
			return err
		}
	case "postgres":
		if err := dfs.PostgresWriteRecords(dataRows); err != nil {
			return err
		}
	case "mongo":
		if err := dfs.MongodbWriteRecords(dataRows); err != nil {
			return err
		}
	default:
		return fmt.Errorf("Not supported: %v", dfs.Kind)
	}
	return nil

}

func (dfs DFStore) ReadRecords(filters []dataframe.F, limit int) ([][]string, error) {
	var res [][]string
	var err error

	switch dfs.Kind {
	case "redis":
		if res, err = dfs.RedisReadRecords(filters, limit); err != nil {
			return nil, err
		}
	case "postgres":
		if res, err = dfs.PostgresReadRecords(filters , limit); err != nil {
			return nil, err
		}
	case "mongo":
		if res, err = dfs.MongodbReadRecords(filters, limit); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Not supported: %v", dfs.Kind)
	}
	return res, nil
}

func (dfs DFStore) RedisWriteRecords(dataRows [][]string) error {
	if dfs.Kind != "redis" {
		return fmt.Errorf("expect kind redis, got %s", dfs.Kind)
	}
	if dfs.RedisClient == nil {
		return fmt.Errorf("RedisClient not initialized")
	}
	cNames := []string{}
	cLen := 0
	var err error
	var pairs []interface{}
	pipe := dfs.RedisClient.TxPipeline()
	for i, row := range dataRows {
		if i == 0 {
			cNames = row
			cLen = len(cNames)
			if cLen < 1 {
				return fmt.Errorf("not enough columns")
			}
			
			columns := strings.Join(cNames, ",")
			key := fmt.Sprintf("schema:%s", dfs.TableName)
			dfs.RedisClient.Set(key, columns, 0)
			continue
		}
		if len(row) != cLen {
			return fmt.Errorf("row %d has %d columns, expected %d", i, len(row), cLen)
		}
		for j, val := range row {
			key := fmt.Sprintf("%s:%d:%s", dfs.TableName, i, cNames[j])
			pairs = append(pairs, key, val)
		}
		pipe.MSet(pairs...)
	
	}
	_, err = pipe.Exec()
	return err
}

func (dfs DFStore) PostgresCreateTable(tablename, schema  string) error {
	if dfs.Kind != "postgres" {
		return fmt.Errorf("expect kind postgres, got %s", dfs.Kind)
	}
	if dfs.PostgresClient == nil {
		return fmt.Errorf("PostgresClient not initialized")
	}
	qStr := "CREATE TABLE IF NOT EXISTS " + tablename + " ( " + schema + "  )"
	q.Q(qStr)
	_, err := dfs.PostgresClient.Query(qStr)
        
	return err
}

func wrapValue(val string) string {
	if val == "" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", val)
}

func parenthesize(val string) string {
	if val == "" {
		return "NULL"
	}
	return fmt.Sprintf("(%s)", val)
}

// docker exec -it postgresql psql -h localhost -p 5432 -U pguser -W -d testdb
// CREATE DATABASE dfstore1;
// CREATE TABLE IF NOT EXISTS schema ( tablename VARCHAR(128) PRIMARY KEY, columns VARCHAR(255) NOT NULL ); 

func (dfs DFStore) PostgresWriteRecords(dataRows [][]string) error {
	if dfs.Kind != "postgres" {
		return fmt.Errorf("expect kind postgres, got %s", dfs.Kind)
	}
	if dfs.PostgresClient == nil {
		return fmt.Errorf("PostgresClient not initialized")
	}
	var cNames []string
	cLen := 0
	var err error
	columns := ""

	dfs.PostgresCreateTable("schema", "tablename VARCHAR(128) PRIMARY KEY, columns VARCHAR(255) NOT NULL")
	dfs.PostgresCreateTable(dfs.TableName, dataRows[0][0] + " VARCHAR(128) PRIMARY KEY" + strings.Join(dataRows[0], " VARCHAR(128),") + " VARCHAR(128)")

	for i, row := range dataRows {
		if i == 0 {
			cNames = row
			cLen = len(cNames)
			if cLen < 1 {
				return fmt.Errorf("not enough columns")
			}
			columns = strings.Join(cNames, ",")
			q.Q(columns)
			qStr := "INSERT INTO schema (tablename, columns) VALUES " +
				parenthesize(wrapValue(dfs.TableName) +  "," + wrapValue(columns))
			q.Q(qStr)
			_, err = dfs.PostgresClient.Query(qStr)
			if err != nil {
				//return err
				q.Q(err)
			}

			continue
		}
		if len(row) != cLen {
			return fmt.Errorf("row %d has %d columns, expected %d", i, len(row), cLen)
		}
		wrappedRow := []string{}
		for _, val := range row {
			wrappedRow = append(wrappedRow, wrapValue(val))
		}

		value := parenthesize(strings.Join(wrappedRow, ","))
		qStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", dfs.TableName, columns, value)
		q.Q(qStr)
		_, err = dfs.PostgresClient.Query(qStr)
		if err != nil {
			q.Q(err)
		}
	}
	
	return err
}


func (dfs DFStore) MongodbWriteRecords(dataRows [][]string) error {
	if dfs.Kind != "mongo" {
		return fmt.Errorf("expect kind mongodb, got %s", dfs.Kind)
	}
	if dfs.MongodbClient == nil {
		return fmt.Errorf("MongodbClient not initialized")
	}
	collection := dfs.MongodbClient.Database(dfs.DBName).Collection(dfs.TableName)
	
	bsonRows := make([]interface{}, 1)
	cNames := []string{}
	cLen := 0
	var err error
	
	for i, row := range dataRows {
		if i == 0 {
			cNames = row
			cLen = len(cNames)
			if cLen < 1 {
				return fmt.Errorf("not enough columns")
			}
			continue
		}
		if len(row) != cLen {
			return fmt.Errorf("row %d has %d columns, expected %d", i, len(row), cLen)
		}

		kvs := []string{}
		for j := 0; j < cLen; j++ {
			kv := fmt.Sprintf(`"%s": "%s"`, cNames[j], row[j])
			kvs = append(kvs, kv)
		}
		
		jsonD := fmt.Sprintf(`{"id": %d, %s}`, i, strings.Join(kvs, ","))
		var bRow  interface{}
		err = bson.UnmarshalExtJSON([]byte(jsonD), false, bRow)
		if err != nil {
			return fmt.Errorf("bson UnmarshalExtJSON error, %v", err)
		}
		bsonRows = append(bsonRows, bRow)
		
	}
	_, err = collection.InsertMany(dfs.Ctx, bsonRows)
	if err != nil {
		return err
	}
	return nil
}


func (dfs DFStore) RedisReadRecords(filters []dataframe.F, limit int) ([][]string, error) {
	if dfs.Kind != "redis" {
		return nil,fmt.Errorf("expect kind redis, got %s", dfs.Kind)
	}
	if dfs.RedisClient == nil {
		return nil,fmt.Errorf("RedisClient not initialized")
	}

	if len(filters) < 1 {
		// fetch columns from schema table
		columns, err := dfs.RedisClient.Get("schema:" + dfs.TableName).Result()
		if err != nil {
			return nil, err
		}
		cNames := strings.Split(columns, ",")
		for _, cN := range cNames {
			filters = append(filters, dataframe.F{Colname: cN, Comparator: ""})
		}
	}
	//TODO validate filters against schema columns
	var keys []string
	var results [][]string

	for _, filt := range filters {
		keys = append(keys, filt.Colname)
	}
	results = append(results, keys)

	for i := 0; len(results) < limit; i++ {
		keys = []string{}
		for _, filt := range filters {
			key := dfs.TableName + ":" + strconv.Itoa(i) + ":" + filt.Colname	
			keys = append(keys, key)			
		}
		vals, err := dfs.RedisClient.MGet(keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("MGet error, %v",err)
		}
		ss := make([]string, len(vals))
		for i,v := range vals {
			//ss[i] = v.(string)
			ss[i] = fmt.Sprintf("%v", v)
		}
		results = append(results,ss)
	}
	df := dataframe.LoadRecords(results)
	
	for _, filt := range filters {
		if filt.Comparator != "" {
			df = df.Filter(filt)
		}
	}
	
	return df.Records(), nil
}

func compTranslate(comp string) string {
	switch comp {
	case "==":
		return "="
	default:
		return comp
	} 
}

func (dfs DFStore) PostgresReadRecords(filters []dataframe.F, limit int) ([][]string, error) {
	if dfs.Kind != "postgres" {
		return nil, fmt.Errorf("expect kind postgres, got %s", dfs.Kind)
	}
	if dfs.PostgresClient == nil {
		return nil, fmt.Errorf("PostgresClient not initialized")
	}
	
	var columns, conditions []string
	
	for _, filt := range filters {
		columns = append(columns, filt.Colname)
		if filt.Comparator == "" {
			continue
		}
		//TODO In, Function cases AND/OR
		conditions = append(conditions,
			fmt.Sprintf("%s %s '%s'", filt.Colname, compTranslate(string(filt.Comparator)), filt.Comparando))
	}
	q.Q(conditions)
	qStr := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(columns, ","), dfs.TableName,
						strings.Join(conditions, " AND "))
	q.Q(qStr)
	rows, err := dfs.PostgresClient.Query(qStr)
	if err != nil {
		q.Q(err)
		return nil,err
	}
	defer rows.Close()
	
	var results [][]string
	fields := make([]interface{}, len(columns) )
	q.Q(fields)
	results = append(results, columns)
	refs := make([]any, len(columns))
	for i := range fields {
		f := &fields[i]
		*f = new(string)
		refs[i] = f
	}
	for rows.Next() {
		if err:= rows.Scan(refs...); err != nil {
			q.Q(err)
			return nil, err
		}
		ss := make([]string, len(refs))
		for i, f := range refs {
			ss[i] = fmt.Sprintf("%v", f)
			//ss[i] = f.(string)
		}
		results = append(results, ss)
		if len(results) > limit {
			break
		}
	}
	q.Q(results)
	return results,nil
}

func (dfs DFStore) MongodbReadRecords(filters []dataframe.F, limit int) ([][]string, error) {
	if dfs.Kind != "mongo" {
		return nil,fmt.Errorf("expected mongodb, got %s", dfs.Kind)
	}
	if dfs.MongodbClient == nil {
		return nil,fmt.Errorf("MongodbClient not initialized")
	}
	collection := dfs.MongodbClient.Database(dfs.DBName).Collection(dfs.TableName)
	
	findOptions := options.Find()
	var bfilters []bson.D

	for _, filt := range filters {
		if filt.Comparator == "" {
			continue
		}
		bfilter := bson.D{
			{filt.Colname, filt.Comparando},
		}
		bfilters = append(bfilters, bfilter)
	}
	cur, err := collection.Find(dfs.Ctx, bfilters, findOptions)
	if err != nil {
		return nil, err
	}
	defer cur.Close(dfs.Ctx)
	var elements []bson.M
	for cur.Next(dfs.Ctx) {
		var elem bson.M
		err := cur.Decode(&elem)

		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)
		if len(elements) > limit {
			break
		}
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	var columns []string
	for _, filt := range filters {
		columns = append(columns, filt.Colname)
	}
	var results [][]string
	
	results = append(results, columns)
	for _, elem := range elements {
		var row []string
		for _, key := range filters {
			row = append(row, elem[key.Colname].(string))
		}
		results = append(results, row)
	}

	return results, nil
}
