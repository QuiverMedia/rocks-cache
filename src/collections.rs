use std::sync::Arc;
use std::io::Cursor;
use time;
use std::fmt;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use rocksdb::{DB, DBIterator, IteratorMode, ColumnFamily, CompactionDecision, MergeOperands};
use bincode::{serialize_into, serialize, deserialize, Infinite};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use serde::{Serialize};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

use error::Error;

const HDR_LEN: usize = 8;

#[derive(Serialize, Deserialize, Clone)]
pub enum UnaryOps<K> {
    Add(K),
    Remove(K),
    Insert(usize, K),
    Del(usize),
    Replace(usize, K),
    Push(K),
    Pop,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum MapOps<K, V> {
    Put(K, V),
    Update(K, V),
    Del(K),
}

impl<K> fmt::Display for UnaryOps<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self:: UnaryOps::*;
        match self {
            &Add(_) => write!(f, "Add<K>"),
            &Remove(_) => write!(f, "Remove<K>"),
            &Insert(_,_) => write!(f, "Insert<usize,K>"),
            &Del(_) => write!(f, "Del<usize>"),
            &Replace(_,_) => write!(f, "Replace<usize, K>"),
            &Push(_) => write!(f, "Push<K>"),
            &Pop  => write!(f, "Pop"),
        }
    }
}

impl<K,V> fmt::Display for MapOps<K,V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self:: MapOps::*;
        match self {
            &Put(_,_) => write!(f, "Put<K,V>"),
            &Update(_,_) => write!(f, "Update<K,V>"),
            &Del(_) => write!(f, "Del<K>"),
        }
    }
}

#[derive(Clone)]
pub struct Kv {
    db: Arc<DB>,
    cf: ColumnFamily,
}

#[derive(Clone)]
pub struct Set<K> {
    db: Arc<DB>,
    cf: ColumnFamily,
    p: PhantomData<K>,
}

#[derive(Clone)]
pub struct Map<K, V> {
    db: Arc<DB>,
    cf: ColumnFamily,
    pk: PhantomData<K>,
    pv: PhantomData<V>,
}

#[derive(Clone)]
pub struct List<K> {
    db: Arc<DB>,
    cf: ColumnFamily,
    p: PhantomData<K>,
}


unsafe impl Send for Kv {}
unsafe impl Sync for Kv {}
unsafe impl<K> Send for Set<K> {}
unsafe impl<K> Sync for Set<K> {}
unsafe impl<K,V> Send for Map<K,V> {}
unsafe impl<K,V> Sync for Map<K,V> {}
unsafe impl<K> Send for List<K> {}
unsafe impl<K> Sync for List<K> {}

impl Kv {
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Kv {
        Kv { db, cf }
    }
    /// get
    ///
    /// Get a `Serialize` value at the corresponding key
    /// if the ttl for the record is expired, Ok(None) is returned
    /// If there is no value found for the key,  Ok(None) is returned
    pub fn get<V>(&self, key: &[u8]) -> Result<Option<V>, Error>
    where
        V: DeserializeOwned
    {
        let res = self.db.get_cf(self.cf, key)?;
        if let Some(inbuf) = res {
            //println!("{:?}, {:?}", key, inbuf[..]);
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: V = deserialize(&inbuf[HDR_LEN..]).map_err(Error::from)?;
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
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let kbuf : Vec<u8> = serialize(&key, Infinite).map_err(Error::from)?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: V = deserialize(&inbuf[HDR_LEN..]).map_err(Error::from)?;
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
    /// If ttl is None, then the record won't expire
    pub fn put<'de, V>(&self, key: &[u8], val: V, ttl: Option<i64>) -> Result<(), Error>
    where
        V: Serialize + DeserializeOwned,
    {
        let mut vbuf : Vec<u8> = vec![]; 
        set_ttl(&mut vbuf, ttl)?;
        serialize_into(&mut vbuf, &val, Infinite).map_err(Error::from)?;
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
    /// If ttl is None, then the record won't expire
    pub fn put_mp<'de, K, V>(&self, key: K, val: V, ttl: Option<i64>) -> Result<(), Error>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(Error::from)?;
        let mut vbuf: Vec<u8> = vec![];
        set_ttl(&mut vbuf, ttl)?;
        serialize_into(&mut vbuf, &val, Infinite)?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(Error::from)
    }

    /// put_raw
    ///
    /// Puts a value into the table at the corresponding key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    pub fn put_raw(&self, key: &[u8], val: &[u8], ttl: Option<i64>) -> Result<(), Error> {
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
        K: Serialize + DeserializeOwned,
    {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(Error::from)?; 
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

impl<K> Set<K> 
    where K : Serialize + DeserializeOwned + Ord + Clone
{
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Set<K> {
        Set { db, cf, p: PhantomData }
    }

    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>> {
        let nops = operands.size_hint().0;
        let mut result: Vec<UnaryOps<K>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<UnaryOps<K>>>(op) {
                for e in k.into_iter() {
                    result.push(e);
                }
            } else {
                debug!("Failed to deserialize operand");
                continue;
            }
        }
        serialize(&result[..], Infinite).ok()
    }

    pub fn full_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>> {

        let empty = vec![];
        let mut result = existing_val.and_then(|ev| deserialize::<BTreeSet<K>>(ev).ok()).unwrap_or(BTreeSet::<K>::new());

        let ops = 
            operands.flat_map(|op| {
                if let Ok(k) = deserialize::<Vec<UnaryOps<K>>>(op) {
                    k.into_iter()
                } else {
                    empty.clone().into_iter()
                }
            });

        for op in ops {
            match op {
                UnaryOps::Add(k) => { result.insert(k); },
                UnaryOps::Remove(ref k) => { result.remove(k); },
                _ => { debug!("Invalid op supplied to Set: {}", op) },
            }
        }

        serialize(&result, Infinite).map_err(|e| debug!("failed to serialize result {:?}", e)).ok()
    }
}

impl<K,V> Map<K,V>
    where K: Serialize + DeserializeOwned + Ord + Clone,
          V: Serialize + DeserializeOwned + Clone,
          V: 
{
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Map<K,V> {
        Map { db, cf, pk: PhantomData, pv: PhantomData }
    }
    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>> 
    {
        let nops = operands.size_hint().0;
        let mut result: Vec<MapOps<K,V>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<MapOps<K,V>>>(op) {
                for e in k.into_iter() {
                    result.push(e);
                }
            } else {
                debug!("Failed to deserialize operand");
                continue;
            }
        }
        serialize(&result[..], Infinite).ok()
    }
    
    pub fn full_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>>
    {
        let empty = vec![];
        let mut result = existing_val.and_then(|ev| deserialize::<BTreeMap<K,V>>(ev).ok()).unwrap_or(BTreeMap::<K,V>::new());

        let ops = 
            operands.flat_map(|op| {
                if let Ok(k) = deserialize::<Vec<MapOps<K,V>>>(op) {
                    k.into_iter()
                } else {
                    empty.clone().into_iter()
                }
            });

        for op in ops {
            match op {
                MapOps::Put(k, v) => { result.insert(k, v); }
                MapOps::Update(k, v) => { result.insert(k, v); }
                MapOps::Del(ref k) => { result.remove(k); }
            }
        }

        serialize(&result, Infinite).map_err(|e| debug!("failed to serialize result {:?}", e)).ok()
    }
}

impl<V> List<V>
    where V : Serialize + DeserializeOwned + Clone {
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> List<V> {
        List { db, cf, p: PhantomData }
    }
    
    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>> {
        let nops = operands.size_hint().0;
        let mut result: Vec<UnaryOps<V>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<UnaryOps<V>>>(op) {
                for e in k.into_iter() {
                    result.push(e);
                }
            } else {
                debug!("Failed to deserialize operand");
                continue;
            }
        }
        serialize(&result[..], Infinite).ok()
    }

    pub fn full_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
        ) -> Option<Vec<u8>> {

        let empty = vec![];
        let mut result = existing_val.and_then(|ev| deserialize::<VecDeque<V>>(ev).ok()).unwrap_or(VecDeque::<V>::new());

        let ops = 
            operands.flat_map(|op| {
                if let Ok(k) = deserialize::<Vec<UnaryOps<V>>>(op) {
                    k.into_iter()
                } else {
                    empty.clone().into_iter()
                }
            });

        for op in ops {
            match op {
                UnaryOps::Insert(idx, v) => { result.insert(idx, v); },
                UnaryOps::Del(idx) => { result.remove(idx); },
                UnaryOps::Replace(idx, v) => { result.push_back(v); result.swap_remove_back(idx); },
                UnaryOps::Push(v) => { result.push_back(v) }
                UnaryOps::Pop => { result.pop_front(); }
                _ => { debug!("Invalid op supplied to Set: {}", op) },
            }
        }

        serialize(&result, Infinite).map_err(|e| debug!("failed to serialize result {:?}", e)).ok()
    }
}

fn ttl_expired(inbuf: &[u8]) -> Result<bool, Error> {
    let ttl = {
        Cursor::new(inbuf).read_i64::<LittleEndian>()?
    };
    Ok(ttl < time::get_time().sec)
}

fn set_ttl(vbuf: &mut Vec<u8>, ttl: Option<i64>) -> Result<(), Error> {

    let end = if let Some(t) = ttl {
        time::get_time().sec + t
    } else {
       ::std::i64::MAX
    };
    vbuf.write_i64::<LittleEndian>(end).map_err(Error::from)
}

pub fn ttl_filter(_level: u32, _key: &[u8], value: &[u8]) -> CompactionDecision {
    use CompactionDecision::*;
    match ttl_expired(value) {
        Ok(true) => Keep,
        Ok(false) => Remove,
        Err(_) => Keep,
    }
}

