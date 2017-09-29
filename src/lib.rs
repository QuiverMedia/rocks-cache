
extern crate rocksdb;
extern crate serde;
extern crate rmp_serde as rmps;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::io::Write;

pub use serde::{Deserialize, Serialize};
pub use rocksdb::{DB, DBVector, DBIterator, IteratorMode, MergeOperands, Options,
                  Error as RDBError};
use rocksdb::ColumnFamily;
use rmps::{Deserializer, Serializer};
use std::error::Error as StdError;

pub struct InitDb {
    pub db: DB,
    tables: Vec<String>,
    opts: Options,
}

impl InitDb {
    pub fn new<P: AsRef<Path>>(path: &Path) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let cfs = DB::list_cf(&opts, path)?;
        let newcfs = cfs.clone();
        let fams: Vec<&str> = newcfs.iter().map(|a| a.as_str()).collect();
        let db = DB::open_cf(&opts, path, &fams)?;
        Ok(InitDb {
            db,
            tables: cfs,
            opts,
        })
    }

    pub fn create_table(&mut self, name: &str) -> Result<(), Error> {
        self.db.create_cf(name, &self.opts)?;

        self.tables.push(name.to_owned());
        Ok(())
    }

    pub fn start(self) -> Db {
        let db = Arc::new(self.db);
        let tables: HashMap<String, Table> = self.tables
            .into_iter()
            .map(|cf| {
                db.cf_handle(cf.as_str()).map(|f| {
                    (
                        cf,
                        Table {
                            cf: f,
                            db: db.clone(),
                        },
                    )
                })
            })
            .flat_map(|cf| cf.into_iter())
            .collect();
        Db { tables }
    }
}

pub struct Db {
    tables: HashMap<String, Table>,
}

impl Db {
    pub fn get_table(&self, name: &str) -> Option<Table> {
        self.tables.get(name).map(|t| t.clone())
    }
}

#[derive(Clone)]
pub struct Table {
    db: Arc<DB>,
    cf: ColumnFamily,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    pub fn get<'de, V>(&self, key: &[u8]) -> Result<Option<V>, Error>
    where
        V: Serialize + Deserialize<'de>,
    {
        let res = self.db.get_cf(self.cf, key)?;
        let inbuf = match res {
            Some(val) => {
                if val.len() < 9 {
                    return Ok(None);
                }
                val
            }
            None => return Ok(None),
        };
        let mut de = Deserializer::new(&inbuf[8..]);
        Deserialize::deserialize(&mut de).map_err(Error::from)
    }

    pub fn get_mp<'de, K, V>(&self, key: K) -> Result<Option<V>, Error>
    where
        K: Serialize + Deserialize<'de>,
        V: Serialize + Deserialize<'de>,
    {
        let mut kbuf = Vec::<u8>::new();
        key.serialize(&mut Serializer::new(&mut kbuf))?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        let inbuf = match res {
            Some(val) => {
                if val.len() < 9 {
                    return Ok(None);
                }
                val
            }
            None => return Ok(None),
        };
        let mut de = Deserializer::new(&inbuf[8..]);
        Deserialize::deserialize(&mut de).map_err(Error::from)
    }

    pub fn get_raw(&self, key: &[u8]) -> Result<Option<DBVector>, Error> {
        self.db.get_cf(self.cf, key).map_err(Error::from)
    }

    pub fn put<'de, K, V>(&self, key: &[u8], val: V) -> Result<(), Error>
    where
        V: Serialize + Deserialize<'de>,
    {
        let mut vbuf = Vec::<u8>::new();
        val.serialize(&mut Serializer::new(&mut vbuf))?;
        self.db.put_cf(self.cf, key, vbuf.as_slice()).map_err(
            Error::from,
        )
    }

    pub fn put_mp<'de, K, V>(&self, key: K, val: V) -> Result<(), Error>
    where
        K: Serialize + Deserialize<'de>,
        V: Serialize + Deserialize<'de>,
    {
        let mut kbuf = Vec::<u8>::new();
        let mut vbuf = Vec::<u8>::new();
        key.serialize(&mut Serializer::new(&mut kbuf))?;
        val.serialize(&mut Serializer::new(&mut vbuf))?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(Error::from)
    }

    pub fn put_raw(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        self.db.put_cf(self.cf, key, val).map_err(Error::from)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete_cf(self.cf, key).map_err(Error::from)
    }

    pub fn delete_mp<'de, K, V>(&self, key: K) -> Result<(), Error>
    where
        K: Serialize + Deserialize<'de>,
    {
        let mut kbuf = Vec::<u8>::new();
        key.serialize(&mut Serializer::new(&mut kbuf))?;
        self.db.delete_cf(self.cf, kbuf.as_slice()).map_err(
            Error::from,
        )
    }

    pub fn iter(&self, mode: IteratorMode) -> Result<DBIterator, Error> {
        self.db.iterator_cf(self.cf, mode).map_err(Error::from)
    }
}

#[derive(Debug, Clone)]
pub struct Error {
    pub description: String,
}

/* 
impl StdError for Error {
    fn description(&self) -> &str {
        self.description.as_str()
    }
}
*/

impl<'a, E: StdError + 'a> From<E> for Error {
    fn from(e: E) -> Error {
        Error { description: e.description().to_owned() }
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
