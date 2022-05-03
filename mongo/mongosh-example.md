### Install mongosh

https://www.mongodb.com/docs/mongodb-shell/install/

### Example usage of mongosh

```
$ mongosh mongodb://root:rootpass@localhost:27017

test> db.dropDatabase('test')


test> use recordings
switched to db recordings

recordings> db.createCollection('table1')

recordings> db.getCollectionNames()

recordings>  db.createUser( { user: 'root',pwd: 'rootpass', roles: [ { role: "readWrite", db: "dfstore1"} ] } )

recordings> db.getUsers()

recordings> db.getCollection('albums').find().forEach(printjson)
{
  _id: ObjectId("000000000000000000000000"),
  artist: 'John Coltrane',
  price: 58.99,
  title: 'Blue Train'
}
{ _id: 1, title: 'Blue Train', artist: 'John Coltrane', price: 56.99 }
{ _id: 2, title: 'Giant Steps', artist: 'John Coltrane', price: 63.99 }
{ _id: 3, title: 'Jeru', artist: 'Gerry Mulligan', price: 17.99 }
{
  _id: 4,
  title: 'Sarah Vaughan',
  artist: 'Sarah Vaughan',
  price: 34.98
}

recordings> 
```

