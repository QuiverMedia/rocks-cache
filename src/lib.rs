
//! A simple facade over [rocksdb](https://crates.io/crates/rocksdb) which aims to provide TTL expiration, strong
//! typing, and easier table (column family) support.
//!
//! Example
//! ```rust
//!
//! extern crate serde;
//! extern crate tempdir;
//!
//! use serde::{Deserialize, Serialize};
//! use rocks_cache::{DbBuilder};
//! use tempdir::TempDir;
//! use std::collections::HashMap;
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
//! struct User {
//!     name: String,
//!     props : HashMap<String, String>,
//!     number: u64,
//! }
//!
//!
//! let dir = TempDir::new("rocks").unwrap();
//! let mut idb = DbBuilder::new(dir.path()).unwrap();
//!
//! // all tables you plan to use at runtime must me
//! // called out before `build`;
//! idb.create_table("user").unwrap();
//! idb.create_table("some").unwrap();
//! let db = idb.build();
//!
//! // after `build` we get a working table handle
//! // which can be safely used between threads
//! let user_tbl = db.get_table("account").unwrap();
//!
//! // construct our mascot
//! let usr = User {
//!     name: "Odie".to_owned(),
//!     props: vec![("age", "91"), ("age metric", "dog years"), ("favorite food", "yes")].iter().map(|(a, b)| (a.to_owned(), b.to_owned())).collect(),
//!     number: 42
//! };
//!
//! // Insert into the db with a ttl of 5 minutes.
//! user_tbl.put(b"names/john", usr, 300).unwrap();
//!
//! // fetch it as the same type we inserted
//! let retrieved : User = user_tbl.get("names/john").unwrap().unwrap();
//! assert_eq!(usr, retrieved);
//!
//!```

extern crate rocksdb;
extern crate serde;
extern crate rmp_serde as rmps;
extern crate byteorder;
extern crate time;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate tempdir;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::error::Error as StdError;
use std::io::Cursor;

pub use serde::{Deserialize, Serialize};
pub use rocksdb::{DB, DBVector, DBIterator, IteratorMode, Options};
use rocksdb::{ColumnFamily, CompactionDecision};
use rmps::{Deserializer, Serializer};
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};

const HDR_LEN: usize = 8;

pub struct DbBuilder {
    pub db: DB,
    tables: Vec<String>,
    opts: Options,
}

impl DbBuilder {
    /// Construct a new `DbBuilder`
    ///
    /// `DbBuilder` is actually a database in its proto form.
    /// Tables may only be added/removed to the database environment in this stage.
    /// After you have declared the necessary tables, execute `build()` to
    /// transform the `DbBuilder` into its executable form.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compaction_filter("ttl", ttl_filter);
        DbBuilder::new_opts(path, opts)
    }

    /// Construct a new `DbBuilder` with `Options`
    ///
    /// `DbBuilder` is actually a database in its proto form.
    /// This exposes a way to set `rocksdb::Options` and pas them in directly.
    /// Tables may only be added/removed to the database environment in this stage.
    /// After you have declared the necessary tables, execute `build()` to
    /// transform the `DbBuilder` into its executable form.
    pub fn new_opts<P: AsRef<Path>>(path: P, mut opts: Options) -> Result<Self, Error> {
        opts.create_if_missing(true);
        opts.set_compaction_filter("ttl", ttl_filter);
        let cfs = DB::list_cf(&opts, &path).unwrap_or(vec![]);
        let ncfs = cfs.clone();
        let fams: Vec<&str> = cfs.iter().map(|a| a.as_str()).collect();
        let db = DB::open_cf(&opts, &path, &fams)?;

        Ok(DbBuilder {
            db,
            tables: ncfs,
            opts,
        })
    }

    /// Adds a new `Table` to the Datbase environment.
    ///
    /// In RocksDB parliance, this is actually a ColumnFamily.
    /// Multiple ColumnFamily's can actually be written/read atomically
    /// in a single operation, but this interface doesn't support it yet.
    pub fn create_table(&mut self, name: &str) -> Result<(), Error> {
        if self.tables.iter().any(|t| t == name) {
            Ok(())
        } else {
            self.db.create_cf(name, &self.opts)?;
            self.tables.push(name.to_owned());
            Ok(())
        }
    }

    /// Convert the DbBuilder into a running Db
    ///
    /// This function "freezes" the database environment from making any
    /// changes such as adding and removing tables. It also transforms
    /// the struct into something that can be safely used in separate threads.
    pub fn build(self) -> Db {
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

#[derive(Clone)]
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
    /// get
    ///
    /// Get a `Serialize` value at the corresponding key
    /// if the ttl for the record is expired, Ok(None) is returned
    /// If there is no value found for the key,  Ok(None) is returned
    pub fn get<'de, V>(&self, key: &[u8]) -> Result<Option<V>, Error>
    where
        V: Serialize + Deserialize<'de>,
    {
        let res = self.db.get_cf(self.cf, key)?;
        if let Some(inbuf) = res {
            //println!("{:?}, {:?}", key, inbuf[..]);
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let mut de = Deserializer::new(&inbuf[HDR_LEN..]);
            let v: V = Deserialize::deserialize(&mut de).map_err(Error::from)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    /// get_mp
    ///
    /// Get a `Serialize` value at the corresponding `Serialize` key
    /// if the ttl for the record is expired, Ok(None) is returned
    /// If there is no value found for the key,  Ok(None) is returned
    pub fn get_mp<'de, K, V>(&self, key: K) -> Result<Option<V>, Error>
    where
        K: Serialize + Deserialize<'de>,
        V: Serialize + Deserialize<'de>,
    {
        let mut kbuf = Vec::<u8>::new();
        key.serialize(&mut Serializer::new(&mut kbuf))?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let mut de = Deserializer::new(&inbuf[HDR_LEN..]);
            let v: V = Deserialize::deserialize(&mut de).map_err(Error::from)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    /// get_raw
    ///
    /// Get a value at the corresponding key
    /// if the ttl for the record is expired, Ok(None) is returned
    /// If there is no value found for the key,  Ok(None) is returned
    pub fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let res = self.db.get_cf(self.cf, key).map_err(Error::from)?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            Ok(Some(inbuf[HDR_LEN..].to_vec()))
        } else {
            Ok(None)
        }
    }

    /// put
    ///
    /// Puts a `Serialize` value into the table at the corresponding `key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    pub fn put<'de, V>(&self, key: &[u8], val: V, ttl: i64) -> Result<(), Error>
    where
        V: Serialize + Deserialize<'de>,
    {
        let mut vbuf = Vec::<u8>::new();
        set_ttl(&mut vbuf, ttl)?;
        val.serialize(&mut Serializer::new(&mut vbuf))?;
        //println!("{:?}, {:?}", key, vbuf);
        self.db.put_cf(self.cf, key, vbuf.as_slice()).map_err(
            Error::from,
        )
    }

    /// put_raw
    ///
    /// Puts a `Serialize` value into the table at the corresponding `Serialize` key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    pub fn put_mp<'de, K, V>(&self, key: K, val: V, ttl: i64) -> Result<(), Error>
    where
        K: Serialize + Deserialize<'de>,
        V: Serialize + Deserialize<'de>,
    {
        let mut kbuf = Vec::<u8>::new();
        let mut vbuf = Vec::<u8>::new();
        key.serialize(&mut Serializer::new(&mut kbuf))?;
        set_ttl(&mut vbuf, ttl)?;
        val.serialize(&mut Serializer::new(&mut vbuf))?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(Error::from)
    }

    /// put_raw
    ///
    /// Puts a value into the table at the corresponding key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    pub fn put_raw(&self, key: &[u8], val: &[u8], ttl: i64) -> Result<(), Error> {
        let mut vbuf = Vec::<u8>::with_capacity(val.len() + HDR_LEN);
        set_ttl(&mut vbuf, ttl)?;
        vbuf.extend_from_slice(val);
        self.db.put_cf(self.cf, key, val).map_err(Error::from)
    }

    /// delete
    ///
    /// deletes a record corresponding to the supplied key
    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.delete_cf(self.cf, key).map_err(Error::from)
    }

    /// delete_mp
    ///
    /// delete's a record using a key that is a Serialize type
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

    /// iter
    /// XXX TODO
    /// This iterator doesn't handle the 8 byte timestamp
    /// preceeding all values, so you must filter it out yourself.
    ///
    /// Iterates through the table (or a subrange of the table)
    /// in the manner specified
    pub fn iter(&self, mode: IteratorMode) -> Result<DBIterator, Error> {
        self.db.iterator_cf(self.cf, mode).map_err(Error::from)
    }
}

fn ttl_expired(inbuf: &[u8]) -> Result<bool, Error> {
    let ttl = {
        Cursor::new(inbuf).read_i64::<LittleEndian>()?
    };
    Ok(ttl < time::get_time().sec)
}

fn set_ttl(vbuf: &mut Vec<u8>, ttl: i64) -> Result<(), Error> {
    let end = time::get_time().sec + ttl;
    vbuf.write_i64::<LittleEndian>(end).map_err(Error::from)
}

fn ttl_filter(_level: u32, _key: &[u8], value: &[u8]) -> CompactionDecision {
    use CompactionDecision::*;
    match ttl_expired(value) {
        Ok(true) => Keep,
        Ok(false) => Remove,
        Err(_) => Keep,
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

    extern crate serde;
    extern crate rmp_serde;
    extern crate lipsum;
    extern crate quickcheck;
    extern crate rand;
    extern crate tempdir;

    use std::collections::{HashMap, BTreeMap};
    use std::cell::RefCell;
    use self::lipsum::{LOREM_IPSUM, LIBER_PRIMUS, MarkovChain};
    use self::quickcheck::{Gen, Arbitrary, StdGen};
    use self::rand::thread_rng;
    use std::thread::sleep;
    use std::time::Duration;
    use super::*;

    thread_local! {
		// Markov chain generating lorem ipsum text.
		static LOREM1: RefCell<MarkovChain<'static, rand::ThreadRng>> = {
			let mut chain = MarkovChain::new();
			// The cost of learning increases as more and more text is
			// added, so we start with the smallest text.
			chain.learn(LOREM_IPSUM);
			chain.learn(LIBER_PRIMUS);
			RefCell::new(chain)
		};
		static LOREM2: RefCell<MarkovChain<'static, rand::ThreadRng>> = {
			let mut chain = MarkovChain::new();
			// The cost of learning increases as more and more text is
			// added, so we start with the smallest text.
			chain.learn(LOREM_IPSUM);
			chain.learn(LIBER_PRIMUS);
			RefCell::new(chain)
		}
	}

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct Account {
        auths: HashMap<String, String>,
        number: u64,
        stuff: Vec<bool>,
        streee: String,
    }

    impl Arbitrary for Account {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let stuff: Vec<bool> = Arbitrary::arbitrary(g);
            let number: u64 = Arbitrary::arbitrary(g);
            let authv: HashMap<String, String> = LOREM1.with(|cell1| {
                LOREM2.with(|cell2| {
                    let mut chain1 = cell1.borrow_mut();
                    let mut chain2 = cell2.borrow_mut();
                    chain1
                        .iter()
                        .zip(chain2.iter())
                        .map(|(a, b)| (a.to_owned(), b.to_owned()))
                        .take(100)
                        .collect()
                })
            });
            let streee: String = Arbitrary::arbitrary(g);
            Account {
                stuff,
                number,
                auths: authv.into_iter().collect(),
                streee,
            }
        }
    }


    #[test]
    fn it_works() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_table("account").unwrap();
        idb.create_table("nums").unwrap();
        let db = idb.build();

        let acct_tbl = db.get_table("account").unwrap();
        let _nums_tbl = db.get_table("nums").unwrap();

        let mut gen = StdGen::new(thread_rng(), 1000);

        let accts: Vec<(String, Account)> = Arbitrary::arbitrary(&mut gen);
        let accts: BTreeMap<String, Account> = accts.into_iter().collect();

        for (k, v) in accts.iter() {
            acct_tbl.put(k.as_bytes(), v.clone(), 100).unwrap();
        }

        let mut newaccts = BTreeMap::<String, Account>::new();
        for (k, v) in accts.iter() {
            let newv = acct_tbl.get(k.as_bytes()).unwrap().unwrap();
            assert_eq!(*v, newv);
            //println!("{:?}", newv);
            newaccts.insert(k.clone(), newv);
        }
        assert_eq!(accts, newaccts);
    }

    #[test]
    fn it_honors_ttls() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_table("account").unwrap();
        idb.create_table("nums").unwrap();
        let db = idb.build();

        let acct_tbl = db.get_table("account").unwrap();
        let _nums_tbl = db.get_table("nums").unwrap();

        let mut gen = StdGen::new(thread_rng(), 100);

        let accts: Vec<(String, Account)> = Arbitrary::arbitrary(&mut gen);
        let accts: BTreeMap<String, Account> = accts.into_iter().collect();

        for (k, v) in accts.iter() {
            acct_tbl.put(k.as_bytes(), v.clone(), 4).unwrap();
        }

        let mut newaccts = BTreeMap::<String, Account>::new();
        for (k, v) in accts.iter() {
            let newv = acct_tbl.get(k.as_bytes()).unwrap().unwrap();
            assert_eq!(*v, newv);
            //println!("{:?}", newv);
            newaccts.insert(k.clone(), newv);
        }
        assert_eq!(accts, newaccts);

        sleep(Duration::new(5, 0));

        for (k, _v) in accts.iter() {
            let newv: Option<Account> = acct_tbl.get(k.as_bytes()).unwrap();
            assert_eq!(newv, None);
        }
    }
}
