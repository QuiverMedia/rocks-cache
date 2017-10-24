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
extern crate bincode;
extern crate byteorder;
extern crate time;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate tempdir;

mod collections;
mod error;

use std::collections::HashMap;
use std::path::{PathBuf};
use std::sync::Arc;

pub use error::Error;
pub use collections::{ Kv, Set, Map, List, ttl_filter };
pub use rocksdb::{DB, DBVector, DBIterator, IteratorMode, Options};
use rocksdb::{ColumnFamilyDescriptor, CompactionDecision};

use serde::{Serialize};
use serde::de::DeserializeOwned;


pub struct DbBuilder {
    kvs: Vec<ColumnFamilyDescriptor>,
    sets: Vec<ColumnFamilyDescriptor>,
    maps: Vec<ColumnFamilyDescriptor>,
    lists: Vec<ColumnFamilyDescriptor>,
    opts: Options,
    path: PathBuf,
}

impl DbBuilder {
    /// Construct a new `DbBuilder`
    ///
    /// `DbBuilder` is actually a database in its proto form.
    /// Collections may only be added/removed to the database environment in this stage.
    /// After you have declared the necessary tables, execute `build()` to
    /// transform the `DbBuilder` into its executable form.
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compaction_filter("ttl", ttl_filter);
        DbBuilder::new_opts(path, opts)
    }

    /// Construct a new `DbBuilder` with `Options`
    ///
    /// `DbBuilder` is actually a database in its proto form.
    /// This exposes a way to set `rocksdb::Options` and pas them in directly.
    /// Collections may only be added/removed to the database environment in this stage.
    /// After you have declared the necessary tables, execute `build()` to
    /// transform the `DbBuilder` into its executable form.
    pub fn new_opts<P: Into<PathBuf>>(path: P, opts: Options) -> Result<Self, Error> {
        let kvs = Vec::new();
        let sets = Vec::new();
        let maps = Vec::new();
        let lists = Vec::new();

        Ok(DbBuilder {
            kvs,
            sets,
            maps,
            lists,
            opts,
            path: path.into(),
        })
    }

    /// Adds a new `KV` generic storage table to the Database environment.
    ///
    /// In RocksDB parliance, this is actually a ColumnFamily.
    /// Multiple ColumnFamily's can actually be written/read atomically
    /// in a single operation, but this interface doesn't support it yet.
    pub fn create_kv(&mut self, name: &str) -> Result<(), Error> {

        let fullname = format!("kvs_{}", name);
        if self.kvs.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            let cfd = ColumnFamilyDescriptor::new(fullname, opts);
            self.kvs.push(cfd);
            Ok(())
        }
    }
    
    /// Adds a new `Set` collection to the Database environment.
    ///
    /// In RocksDB parliance, this is actually a ColumnFamily.
    /// Multiple ColumnFamily's can actually be written/read atomically
    /// in a single operation, but this interface doesn't support it yet.
    pub fn create_set<K>(&mut self, name: &str) -> Result<(), Error>
    where K : Serialize + DeserializeOwned + Ord + Clone
    {

        let fullname = format!("set_{}", name);
        if self.sets.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator("set_merge", Set::full_merge::<K>, Some(Set::partial_merge::<K>));
            let cfd = ColumnFamilyDescriptor::new(fullname, opts);
            self.sets.push(cfd);
            Ok(())
        }
    }
    
    /// Adds a new `Map` collection to the Database environment.
    ///
    /// In RocksDB parliance, this is actually a ColumnFamily.
    /// Multiple ColumnFamily's can actually be written/read atomically
    /// in a single operation, but this interface doesn't support it yet.
    pub fn create_map<K, V>(&mut self, name: &str) -> Result<(), Error> 
    where K : Serialize + DeserializeOwned + Ord + Clone,
          V :  Serialize + DeserializeOwned + Ord + Clone,
    {
        let fullname = format!("map_{}", name);
        if self.maps.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator("map_merge", Map::full_merge::<K, V>, Some(Map::partial_merge::<K, V>));
            let cfd = ColumnFamilyDescriptor::new(fullname, opts);
            self.maps.push(cfd);
            Ok(())
        }
    }
   

    /// Adds a new `List` collection to the Database environment.
    ///
    /// In RocksDB parliance, this is actually a ColumnFamily.
    /// Multiple ColumnFamily's can actually be written/read atomically
    /// in a single operation, but this interface doesn't support it yet.
    pub fn create_list<V>(&mut self, name: &str) -> Result<(), Error> 
    where V :  Serialize + DeserializeOwned + Ord + Clone,
    {
        let fullname = format!("lis_{}", name);
        if self.lists.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator("list_merge", List::full_merge::<V>, Some(List::partial_merge::<V>));
            let cfd = ColumnFamilyDescriptor::new(fullname, opts);
            self.maps.push(cfd);
            Ok(())
        }
    }

    /// Convert the DbBuilder into a running Db
    ///
    /// This function "freezes" the database environment from making any
    /// changes such as adding and removing tables. It also transforms
    /// the struct into something that can be safely used in separate threads.
    pub fn build(self) -> Result<Db, Error> {
        let mut kvs = HashMap::<String, Kv>::new();
        let mut sets = HashMap::<String, Set>::new();
        let mut  maps = HashMap::<String, Map>::new();
        let mut lists = HashMap::<String, List>::new();

        let cfds : Vec<ColumnFamilyDescriptor> = self.kvs.into_iter()
            .chain(self.sets.into_iter())
            .chain(self.maps.into_iter())
            .chain(self.lists.into_iter())
            .collect();

        let names : Vec<String> = cfds.iter().map(|cf| cf.name().clone()).collect();

        let db = Arc::new(DB::open_cf_descriptors(&self.opts, self.path, cfds)?);

        for name in names {
            match &name[..3] {
                "kvs" => { if let Some(cf) = db.cf_handle(name.as_str()) { kvs.insert(name[4..].to_string(), Kv::new(db.clone(), cf)); } },
                "set" => { if let Some(cf) = db.cf_handle(name.as_str()) { sets.insert(name[4..].to_string(), Set::new(db.clone(), cf)); } },
                "map" => { if let Some(cf) = db.cf_handle(name.as_str()) { maps.insert(name[4..].to_string(), Map::new(db.clone(), cf)); } },
                "lis" => { if let Some(cf) = db.cf_handle(name.as_str()) { lists.insert(name[4..].to_string(), List::new(db.clone(), cf)); } },
                name => { if let Some(cf) = db.cf_handle(name) { kvs.insert(name.to_string(), Kv::new(db.clone(), cf)); } },
            }
        }

        Ok(Db { kvs, maps, sets, lists })
    }
}

#[derive(Clone)]
pub struct Db {
    kvs: HashMap<String, Kv>,
    maps: HashMap<String, Map>,
    sets: HashMap<String, Set>,
    lists: HashMap<String, List>,
}

impl Db {
    pub fn get_kv(&self, name: &str) -> Option<Kv> {
        self.kvs.get(name).map(|t| t.clone())
    }
    pub fn get_set(&self, name: &str) -> Option<Set> {
        self.sets.get(name).map(|t| t.clone())
    }
    pub fn get_map(&self, name: &str) -> Option<Map> {
        self.maps.get(name).map(|t| t.clone())
    }
    pub fn get_list(&self, name: &str) -> Option<List> {
        self.lists.get(name).map(|t| t.clone())
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
            acct_tbl.put(k.as_bytes(), v.clone(), Some(100)).unwrap();
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
            acct_tbl.put(k.as_bytes(), v.clone(), Some(4)).unwrap();
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
