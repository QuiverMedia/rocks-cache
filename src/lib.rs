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
//! idb.create_kv("user").unwrap();
//! idb.create_kv("some").unwrap();
//! let db = idb.build();
//!
//! // after `build` we get a working table handle
//! // which can be safely used between threads
//! let user_tbl = db.get_kv("account").unwrap();
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
extern crate failure;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate failure_derive;

#[cfg(test)]
extern crate tempdir;

mod collections;
mod error;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub use error::RocksCacheError;
pub use collections::{Kv, Set, Map, List, ttl_filter};
pub use rocksdb::{DB, DBVector, DBIterator, IteratorMode, Options};
use rocksdb::{ColumnFamilyDescriptor, ColumnFamily, CompactionDecision};

use serde::Serialize;
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
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<Self, RocksCacheError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
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
    pub fn new_opts<P: Into<PathBuf>>(path: P, opts: Options) -> Result<Self, RocksCacheError> {
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
    pub fn create_kv(&mut self, name: &str) -> Result<(), RocksCacheError> {

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
    pub fn create_set<K, T>(&mut self, name: &str) -> Result<(), RocksCacheError>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        T: Serialize + DeserializeOwned + Ord + Clone,
    {

        let fullname = format!("set_{}", name);
        if self.sets.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator(
                "set_merge",
                Set::<K, T>::full_merge,
                Some(Set::<K, T>::partial_merge),
            );
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
    pub fn create_map<K, U, V>(&mut self, name: &str) -> Result<(), RocksCacheError>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        U: Serialize + DeserializeOwned + Ord + Clone,
        V: Serialize + DeserializeOwned + Ord + Clone,
    {
        let fullname = format!("map_{}", name);
        if self.maps.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator(
                "map_merge",
                Map::<K, U, V>::full_merge,
                Some(Map::<K, U, V>::partial_merge),
            );
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
    pub fn create_list<K, T>(&mut self, name: &str) -> Result<(), RocksCacheError>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        T: Serialize + DeserializeOwned + Ord + Clone,
    {
        let fullname = format!("lis_{}", name);
        if self.lists.iter().any(|t| t.name().eq(&fullname)) {
            Ok(())
        } else {
            let mut opts = Options::default();
            opts.set_compaction_filter("ttl", ttl_filter);
            opts.set_merge_operator(
                "list_merge",
                List::<K, T>::full_merge,
                Some(List::<K, T>::partial_merge),
            );
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
    pub fn build(self) -> Result<Db, RocksCacheError> {
        let mut kvs = HashMap::<String, ColumnFamily>::new();
        let mut sets = HashMap::<String, ColumnFamily>::new();
        let mut maps = HashMap::<String, ColumnFamily>::new();
        let mut lists = HashMap::<String, ColumnFamily>::new();

        let cfds: Vec<ColumnFamilyDescriptor> = self.kvs
            .into_iter()
            .chain(self.sets.into_iter())
            .chain(self.maps.into_iter())
            .chain(self.lists.into_iter())
            .collect();

        let names: Vec<String> = cfds.iter().map(|cf| cf.name().clone()).collect();
        let db = Arc::new(DB::open_cf_descriptors(&self.opts, self.path, cfds)?);

        for name in names {
            println!("{} -- {}", &name[..3], &name[4..]);
            match &name[..3] {
                "kvs" => {
                    if let Some(cf) = db.cf_handle(name.as_str()) {
                        kvs.insert(name[4..].to_string(), cf);
                    }
                }
                "set" => {
                    if let Some(cf) = db.cf_handle(name.as_str()) {
                        sets.insert(name[4..].to_string(), cf);
                    }
                }
                "map" => {
                    if let Some(cf) = db.cf_handle(name.as_str()) {
                        maps.insert(name[4..].to_string(), cf);
                    }
                }
                "lis" => {
                    if let Some(cf) = db.cf_handle(name.as_str()) {
                        lists.insert(name[4..].to_string(), cf);
                    }
                }
                name => {
                    if let Some(cf) = db.cf_handle(name) {
                        kvs.insert(name.to_string(), cf);
                    }
                }
            }
        }

        Ok(Db {
            db,
            kvs,
            maps,
            sets,
            lists,
        })
    }
}

#[derive(Clone)]
pub struct Db {
    db: Arc<DB>,
    kvs: HashMap<String, ColumnFamily>,
    maps: HashMap<String, ColumnFamily>,
    sets: HashMap<String, ColumnFamily>,
    lists: HashMap<String, ColumnFamily>,
}

impl Db {
    pub fn get_kv(&self, name: &str) -> Option<Kv> {
        self.kvs.get(name).map(
            |cf| Kv::new(self.db.clone(), cf.clone()),
        )
    }
    pub fn get_set<K, T>(&self, name: &str) -> Option<Set<K, T>>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        T: Serialize + DeserializeOwned + Ord + Clone,
    {
        self.sets.get(name).map(|cf| {
            Set::<K, T>::new(self.db.clone(), cf.clone())
        })
    }
    pub fn get_map<K, U, V>(&self, name: &str) -> Option<Map<K, U, V>>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        U: Serialize + DeserializeOwned + Ord + Clone,
        V: Serialize + DeserializeOwned + Clone,
    {
        self.maps.get(name).map(|cf| {
            Map::<K, U, V>::new(self.db.clone(), cf.clone())
        })
    }
    pub fn get_list<K, T>(&self, name: &str) -> Option<List<K, T>>
    where
        K: Serialize + DeserializeOwned + Ord + Clone,
        T: Serialize + DeserializeOwned + Ord + Clone,
    {
        self.lists.get(name).map(|cf| {
            List::<K, T>::new(self.db.clone(), cf.clone())
        })
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

    use std::collections::{HashMap, BTreeMap, BTreeSet, VecDeque};
    use std::cell::RefCell;
    use self::lipsum::{LOREM_IPSUM, LIBER_PRIMUS, MarkovChain};
    use self::quickcheck::{Gen, Arbitrary, StdGen};
    use self::rand::thread_rng;
    use std::thread::sleep;
    use std::time::Duration;
    use super::*;

    thread_local! {
		// Markov chain generating lorem ipsum text.
		static LOREM1: RefCell<MarkovChain<'static, rand::XorShiftRng>> = {
            let rng = rand::XorShiftRng::new_unseeded();
            let mut chain = MarkovChain::new_with_rng(rng);
			// The cost of learning increases as more and more text is
			// added, so we start with the smallest text.
			chain.learn(LOREM_IPSUM);
			chain.learn(LIBER_PRIMUS);
			RefCell::new(chain)
		};
		static LOREM2: RefCell<MarkovChain<'static, rand::StdRng>> = {
            let rng = rand::StdRng::new().unwrap();
			let mut chain = MarkovChain::new_with_rng(rng);
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

        idb.create_kv("account").unwrap();
        idb.create_kv("nums").unwrap();
        let db = idb.build().unwrap();

        let acct_tbl = db.get_kv("account").unwrap();
        let _nums_tbl = db.get_kv("nums").unwrap();

        let mut gen = StdGen::new(thread_rng(), 1000);

        let accts: Vec<(String, Account)> = Arbitrary::arbitrary(&mut gen);
        let accts: BTreeMap<String, Account> = accts.into_iter().collect();

        for (k, v) in accts.iter() {
            acct_tbl.put(k.as_bytes(), v, Some(100)).unwrap();
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

        idb.create_kv("account").unwrap();
        idb.create_kv("nums").unwrap();
        let db = idb.build().unwrap();

        println!("Getting account");
        let acct_tbl = db.get_kv("account").unwrap();
        let _nums_tbl = db.get_kv("nums").unwrap();

        let mut gen = StdGen::new(thread_rng(), 100);

        let accts: Vec<(String, Account)> = Arbitrary::arbitrary(&mut gen);
        let accts: BTreeMap<String, Account> = accts.into_iter().collect();

        println!("Putting abytes");
        for (k, v) in accts.iter() {
            acct_tbl.put(k.as_bytes(), v, Some(4)).unwrap();
        }

        println!("Getting abytes");
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

    #[test]
    fn it_sets_sets() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_set::<String, String>("latin").unwrap();
        let db = idb.build().unwrap();
        let key = "key1".to_string();
        println!("Getting set");
        let tbl = db.get_set::<String, String>("latin").unwrap();

        let mut stuff: Vec<String> = LOREM2.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        tbl.create(&key, &stuff, None).unwrap();

        let sorted_stuff: Vec<String> = stuff
            .iter()
            .map(|s| s.clone())
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
        let results: Vec<String> = tbl.get(&key).unwrap().unwrap().into_iter().collect();

        assert_eq!(results, sorted_stuff);

        for v in stuff.iter() {
            tbl.insert(&key, v).unwrap();
        }

        let results: Vec<String> = tbl.get(&key).unwrap().unwrap().into_iter().collect();

        assert_eq!(results, sorted_stuff);

    }

    #[test]
    fn it_gets_sets() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_set::<String, String>("latin").unwrap();
        let db = idb.build().unwrap();
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        let tbl = db.get_set::<String, String>("latin").unwrap();

        let mut stuff: Vec<String> = LOREM1.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let mut more_stuff: Vec<String> = LOREM2.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let sorted_more = more_stuff.iter().cloned().collect::<BTreeSet<String>>();
        let sorted_stuff = stuff.iter().cloned().collect::<BTreeSet<String>>();
        println!("sorted_stuff == {:?}", sorted_stuff);
        println!("sorted_more == {:?}", sorted_more);

        tbl.create(&key1, &stuff, None).unwrap();

        let results = tbl.get(&key1).unwrap().unwrap();
        assert_eq!(results, sorted_stuff);
        println!("results == {:?}", results);

        for v in more_stuff.iter() {
            tbl.insert(&key2, v).unwrap();
        }

        let more_results = tbl.get(&key2).unwrap().unwrap();
        assert_eq!(more_results, sorted_more);
        println!("more_results == {:?}", more_results);

        for v in more_results.iter() {
            tbl.insert(&key1, v).unwrap();
        }

        let results = tbl.get(&key1).unwrap().unwrap();

        let big_stuff = stuff
            .iter()
            .chain(more_stuff.iter())
            .cloned()
            .collect::<BTreeSet<String>>();

        println!("big_results == {:?}", results);
        assert_eq!(results, big_stuff);

        for v in sorted_more.iter() {
            tbl.remove(&key1, v).unwrap();
        }

        let k1results = tbl.get(&key1).unwrap().unwrap();
        println!("k1results == {:?}", k1results);

        let diff: BTreeSet<String> = sorted_stuff.difference(&sorted_more).cloned().collect();

        assert_eq!(k1results, diff);

        tbl.delete(&key1).unwrap();

        assert_eq!(tbl.get(&key1).unwrap(), None);
    }

    #[test]
    fn it_sets_maps() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_map::<String, String, String>("latin").unwrap();

        let db = idb.build().unwrap();
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        let tbl = db.get_map::<String, String, String>("latin").unwrap();

        // ensure keys are unique
        let mut keys1: BTreeSet<String> = LOREM1.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let mut vals1: Vec<String> = LOREM2.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        // ensure keys are unique
        let mut keys2: BTreeSet<String> = LOREM1.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let mut vals2: Vec<String> = LOREM2.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let entries1: Vec<(String, String)> = keys1.into_iter().zip(vals1.into_iter()).collect();
        let entries2: Vec<(String, String)> = keys2.into_iter().zip(vals2.into_iter()).collect();

        tbl.create(&key1, &entries1, None).unwrap();

        for &(ref k, ref v) in entries2.iter() {
            tbl.upsert(&key2, k, v).unwrap();
        }

        for &(ref k, ref v) in entries1.iter() {
            assert_eq!(tbl.get_val(&key1, k).unwrap(), Some(v.clone()))
        }

        for &(ref k, ref v) in entries2.iter() {
            assert_eq!(tbl.get_val(&key2, k).unwrap(), Some(v.clone()))
        }

    }

    #[test]
    fn it_sets_lists() {
        let dir = tempdir::TempDir::new("rocks").unwrap();
        let mut idb = DbBuilder::new(dir.path()).unwrap();

        idb.create_list::<String, String>("latin").unwrap();
        let db = idb.build().unwrap();
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        let tbl = db.get_list::<String, String>("latin").unwrap();

        let mut stuff: Vec<String> = LOREM1.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        let mut more_stuff: Vec<String> = LOREM2.with(|cell| {
            let mut chain1 = cell.borrow_mut();
            chain1.iter().take(100).map(String::from).collect()
        });

        tbl.create(&key1, &stuff, None).unwrap();

        let results = tbl.get(&key1).unwrap().unwrap();
        assert_eq!(results, stuff.iter().cloned().collect::<VecDeque<String>>());

        for v in more_stuff.iter() {
            tbl.push(&key2, v).unwrap();
        }

        let more_results = tbl.get(&key2).unwrap().unwrap();
        assert_eq!(
            more_results,
            more_stuff.iter().cloned().collect::<VecDeque<String>>()
        );
    }

}
