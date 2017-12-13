use std::sync::Arc;
use std::io::Cursor;
use std::iter::IntoIterator;
use time;
use std::fmt;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use rocksdb::{DB, DBIterator, IteratorMode, ColumnFamily, CompactionDecision, MergeOperands,
              WriteOptions};
use bincode::{serialize_into, serialize, deserialize, Infinite};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;

use error::RocksCacheError;

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
    Upsert(K, V),
    Del(K),
}

impl<K> fmt::Display for UnaryOps<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::UnaryOps::*;
        match self {
            &Add(_) => write!(f, "Add<K>"),
            &Remove(_) => write!(f, "Remove<K>"),
            &Insert(_, _) => write!(f, "Insert<usize,K>"),
            &Del(_) => write!(f, "Del<usize>"),
            &Replace(_, _) => write!(f, "Replace<usize, K>"),
            &Push(_) => write!(f, "Push<K>"),
            &Pop => write!(f, "Pop"),
        }
    }
}

impl<K, V> fmt::Display for MapOps<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::MapOps::*;
        match self {
            &Upsert(_, _) => write!(f, "Update<K,V>"),
            &Del(_) => write!(f, "Del<K>"),
        }
    }
}

#[derive(Clone)]
pub struct Kv {
    pub db: Arc<DB>,
    pub cf: ColumnFamily,
}

#[derive(Clone)]
pub struct Set<K, T> {
    pub db: Arc<DB>,
    pub cf: ColumnFamily,
    p1: PhantomData<K>,
    p2: PhantomData<T>,
}

#[derive(Clone)]
pub struct Map<K, U, V> {
    pub db: Arc<DB>,
    pub cf: ColumnFamily,
    p1: PhantomData<K>,
    p2: PhantomData<U>,
    p3: PhantomData<V>,
}

#[derive(Clone)]
pub struct List<K, T> {
    pub db: Arc<DB>,
    pub cf: ColumnFamily,
    p1: PhantomData<K>,
    p2: PhantomData<T>,
}


unsafe impl Send for Kv {}
unsafe impl Sync for Kv {}
unsafe impl<K, T> Send for Set<K, T> {}
unsafe impl<K, T> Sync for Set<K, T> {}
unsafe impl<K, U, V> Send for Map<K, U, V> {}
unsafe impl<K, U, V> Sync for Map<K, U, V> {}
unsafe impl<K, T> Send for List<K, T> {}
unsafe impl<K, T> Sync for List<K, T> {}

impl Kv {
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Kv {
        Kv { db, cf }
    }
    /// get
    ///
    /// Get a `Serialize` value at the corresponding key
    /// if the ttl for the record is expired, Ok(None) is returned
    /// If there is no value found for the key,  Ok(None) is returned
    pub fn get<V>(&self, key: &[u8]) -> Result<Option<V>, RocksCacheError>
    where
        V: DeserializeOwned,
    {
        let res = self.db.get_cf(self.cf, key)?;
        if let Some(inbuf) = res {
            //println!("{:?}, {:?}", key, inbuf[..]);
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: V = deserialize(&inbuf[HDR_LEN..])?;
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
    pub fn get_mp<'de, K, V>(&self, key: K) -> Result<Option<V>, RocksCacheError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: V = deserialize(&inbuf[HDR_LEN..]).map_err(
                RocksCacheError::from,
            )?;
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
    pub fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksCacheError> {
        let res = self.db.get_cf(self.cf, key).map_err(RocksCacheError::from)?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            Ok(Some(inbuf[HDR_LEN..].to_vec()))
        } else {
            Ok(None)
        }
    }

    /// exists
    ///
    /// Check if there is an entry in the Db for the raw bytes key
    /// if the ttl for the record is expired, Ok(false) is returned
    /// If there is no value found for the key,  Ok(false) is returned
    pub fn exists(&self, key: &[u8]) -> Result<bool, RocksCacheError> {
        let res = self.db.get_cf(self.cf, key).map_err(RocksCacheError::from)?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                Ok(false)
            } else {
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    /// put
    ///
    /// Puts a `Serialize` value into the table at the corresponding bytes `key`
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    /// If ttl is None, then the record won't expire
    pub fn put<'de, V>(&self, key: &[u8], val: V, ttl: Option<i64>) -> Result<(), RocksCacheError>
    where
        V: Serialize + DeserializeOwned,
    {
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, ttl)?;
        serialize_into(&mut vbuf, &val, Infinite).map_err(
            RocksCacheError::from,
        )?;
        //println!("{:?}, {:?}", key, vbuf);
        self.db.put_cf(self.cf, key, vbuf.as_slice()).map_err(
            RocksCacheError::from,
        )
    }

    /// put_mp
    ///
    /// Puts a `Serialize` value into the table at the corresponding `Serialize` key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    /// If ttl is None, then the record won't expire
    pub fn put_mp<'de, K, V>(&self, key: K, val: V, ttl: Option<i64>) -> Result<(), RocksCacheError>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, ttl)?;
        serialize_into(&mut vbuf, &val, Infinite)?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    /// put_raw
    ///
    /// Puts a value into the table at the corresponding key
    /// The value will expire and be removed from the table at ttl seconds from
    /// the function call
    pub fn put_raw(&self, key: &[u8], val: &[u8], ttl: Option<i64>) -> Result<(), RocksCacheError> {
        let mut vbuf = Vec::<u8>::with_capacity(val.len() + HDR_LEN);
        set_ttl(&mut vbuf, ttl)?;
        vbuf.extend_from_slice(val);
        self.db.put_cf(self.cf, key, val).map_err(
            RocksCacheError::from,
        )
    }

    /// delete
    ///
    /// deletes a record corresponding to the supplied key
    pub fn delete(&self, key: &[u8]) -> Result<(), RocksCacheError> {
        self.db.delete_cf(self.cf, key).map_err(
            RocksCacheError::from,
        )
    }

    /// delete_mp
    ///
    /// delete's a record using a key that is a Serialize type
    pub fn delete_mp<'de, K, V>(&self, key: K) -> Result<(), RocksCacheError>
    where
        K: Serialize + DeserializeOwned,
    {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        self.db.delete_cf(self.cf, kbuf.as_slice()).map_err(
            RocksCacheError::from,
        )
    }

    /// iter
    /// XXX TODO
    /// This iterator doesn't handle the 8 byte timestamp
    /// preceeding all values, so you must filter it out yourself.
    ///
    /// Iterates through the table (or a subrange of the table)
    /// in the manner specified
    pub fn iter(&self, mode: IteratorMode) -> Result<DBIterator, RocksCacheError> {
        self.db.iterator_cf(self.cf, mode).map_err(
            RocksCacheError::from,
        )
    }
}

impl<K, T> Set<K, T>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    T: Serialize + DeserializeOwned + Ord + Clone,
{
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Set<K, T> {
        Set {
            db,
            cf,
            p1: PhantomData,
            p2: PhantomData,
        }
    }

    pub fn create<'a, I>(&self, key: &K, values: I, ttl: Option<i64>) -> Result<(), RocksCacheError>
    where
        I: IntoIterator<Item = &'a T>,
        T: 'a,
    {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, ttl)?;
        let val: BTreeSet<&T> = values.into_iter().collect();
        serialize_into(&mut vbuf, &val, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn contains(&self, key: &K, item: &T) -> Result<bool, RocksCacheError> {
        self.get(key).map(|o| {
            o.map(|v| v.contains(item)).unwrap_or(false)
        })
    }

    pub fn get(&self, key: &K) -> Result<Option<BTreeSet<T>>, RocksCacheError> {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: BTreeSet<T> = deserialize(&inbuf[HDR_LEN..]).map_err(
                RocksCacheError::from,
            )?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&self, key: &K) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let opts = WriteOptions::default();
        self.db
            .delete_cf_opt(self.cf, kbuf.as_slice(), &opts)
            .map_err(RocksCacheError::from)
    }

    pub fn set_ttl(&self, key: &K, ttl: i64) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, Some(ttl))?;
        let op: Vec<UnaryOps<T>> = vec![];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn insert(&self, key: &K, item: &T) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Add(item)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn remove(&self, key: &K, item: &T) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Remove(item)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let nops = operands.size_hint().0;
        let mut result: Vec<UnaryOps<T>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<UnaryOps<T>>>(op) {
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
        let (ttl, mut result) = existing_val
            .and_then(|ev| {
                let ttl = Cursor::new(ev).read_i64::<LittleEndian>().ok()?;
                deserialize::<BTreeSet<T>>(&ev[HDR_LEN..]).ok().map(
                    |s| (ttl, s),
                )
            })
            .unwrap_or((::std::i64::MAX, BTreeSet::<T>::new()));

        println!("({:?})", ttl);
        let ops = operands.flat_map(|op| if let Ok(k) = deserialize::<Vec<UnaryOps<T>>>(op) {
            k.into_iter()
        } else {
            empty.clone().into_iter()
        });

        for op in ops {
            match op {
                UnaryOps::Add(k) => {
                    result.insert(k);
                }
                UnaryOps::Remove(ref k) => {
                    result.remove(k);
                }
                _ => debug!("Invalid op supplied to Set: {}", op),
            }
        }

        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        vbuf.write_i64::<LittleEndian>(ttl)
            .map_err(|e| debug!("failed to serialize ttl: {:?}", e))
            .ok()?;
        serialize_into(&mut vbuf, &result, Infinite)
            .map_err(|e| debug!("failed to serialize result {:?}", e))
            .ok()?;
        Some(vbuf)
    }
}

impl<K, U, V> Map<K, U, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    U: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> Map<K, U, V> {
        Map {
            db,
            cf,
            p1: PhantomData,
            p2: PhantomData,
            p3: PhantomData,
        }
    }

    pub fn create<'a, I>(&self, key: &K, values: I, ttl: Option<i64>) -> Result<(), RocksCacheError>
    where
        I: IntoIterator<Item = &'a (U, V)>,
        U: 'a,
        V: 'a,
    {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, ttl)?;
        let val: BTreeMap<&'a U, &'a V> =
            values.into_iter().map(|&(ref u, ref v)| (u, v)).collect();
        serialize_into(&mut vbuf, &val, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn get(&self, key: &K) -> Result<Option<BTreeMap<U, V>>, RocksCacheError> {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: BTreeMap<U, V> = deserialize(&inbuf[HDR_LEN..]).map_err(
                RocksCacheError::from,
            )?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub fn get_val(&self, key: &K, entry_key: &U) -> Result<Option<V>, RocksCacheError> {
        self.get(key).map(|o| {
            o.and_then(|v| v.get(entry_key).map(|v| v.clone()))
        })
    }

    pub fn delete(&self, key: &K) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let opts = WriteOptions::default();
        self.db
            .delete_cf_opt(self.cf, kbuf.as_slice(), &opts)
            .map_err(RocksCacheError::from)
    }

    pub fn set_ttl(&self, key: &K, ttl: i64) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, Some(ttl))?;
        let op: Vec<MapOps<U, V>> = vec![];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn upsert(&self, key: &K, item_key: &U, item_val: &V) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![MapOps::Upsert(item_key, item_val)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn remove(&self, key: &K, item_key: &U) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Remove(item_key)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let nops = operands.size_hint().0;
        let mut result: Vec<MapOps<U, V>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<MapOps<U, V>>>(op) {
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
        let (ttl, mut result) = existing_val
            .and_then(|ev| {
                let ttl = Cursor::new(ev).read_i64::<LittleEndian>().ok()?;
                deserialize::<BTreeMap<U, V>>(&ev[HDR_LEN..]).ok().map(
                    |s| {
                        (ttl, s)
                    },
                )
            })
            .unwrap_or((::std::i64::MAX, BTreeMap::<U, V>::new()));

        println!("({:?})", ttl);
        let ops = operands.flat_map(|op| if let Ok(k) = deserialize::<Vec<MapOps<U, V>>>(op) {
            k.into_iter()
        } else {
            empty.clone().into_iter()
        });

        for op in ops {
            match op {
                MapOps::Upsert(u, v) => {
                    result.insert(u, v);
                }
                MapOps::Del(ref u) => {
                    result.remove(u);
                }
            }
        }

        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        vbuf.write_i64::<LittleEndian>(ttl)
            .map_err(|e| debug!("failed to serialize ttl: {:?}", e))
            .ok()?;
        serialize_into(&mut vbuf, &result, Infinite)
            .map_err(|e| debug!("failed to serialize result {:?}", e))
            .ok()?;
        Some(vbuf)
    }
}

impl<K, T> List<K, T>
where
    K: Serialize + DeserializeOwned + Clone,
    T: Serialize + DeserializeOwned + Clone,
{
    pub fn new(db: Arc<DB>, cf: ColumnFamily) -> List<K, T> {
        List {
            db,
            cf,
            p1: PhantomData,
            p2: PhantomData,
        }
    }

    pub fn create<'a, I>(&self, key: &K, values: I, ttl: Option<i64>) -> Result<(), RocksCacheError>
    where
        I: IntoIterator<Item = &'a T>,
        T: 'a,
    {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, ttl)?;
        {
            let val: VecDeque<&T> = values.into_iter().collect();
            serialize_into(&mut vbuf, &val, Infinite).map_err(
                RocksCacheError::from,
            )?;
        }
        self.db
            .put_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn get(&self, key: &K) -> Result<Option<VecDeque<T>>, RocksCacheError> {
        let kbuf: Vec<u8> = serialize(&key, Infinite).map_err(RocksCacheError::from)?;
        let res = self.db.get_cf(self.cf, kbuf.as_slice())?;
        if let Some(inbuf) = res {
            if ttl_expired(&inbuf[..])? {
                return Ok(None);
            }
            let v: VecDeque<T> = deserialize(&inbuf[HDR_LEN..]).map_err(
                RocksCacheError::from,
            )?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&self, key: &K) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let opts = WriteOptions::default();
        self.db
            .delete_cf_opt(self.cf, kbuf.as_slice(), &opts)
            .map_err(RocksCacheError::from)
    }

    pub fn set_ttl(&self, key: &K, ttl: i64) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        set_ttl(&mut vbuf, Some(ttl))?;
        let op: Vec<UnaryOps<T>> = vec![];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn insert(&self, key: &K, index: usize, item: &T) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Insert(index, item)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn replace(&self, key: &K, index: usize, item: &T) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Replace(index, item)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn remove(&self, key: &K, index: usize) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op: Vec<UnaryOps<T>> = vec![UnaryOps::Del(index)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn push(&self, key: &K, item: &T) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op = vec![UnaryOps::Push(item)];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn pop(&self, key: &K) -> Result<(), RocksCacheError> {
        let kbuf = serialize(&key, Infinite)?;
        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        let op: Vec<UnaryOps<T>> = vec![UnaryOps::Pop];
        serialize_into(&mut vbuf, &op, Infinite).map_err(
            RocksCacheError::from,
        )?;
        self.db
            .merge_cf(self.cf, kbuf.as_slice(), vbuf.as_slice())
            .map_err(RocksCacheError::from)
    }

    pub fn partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &mut MergeOperands,
    ) -> Option<Vec<u8>> {
        let nops = operands.size_hint().0;
        let mut result: Vec<UnaryOps<T>> = Vec::with_capacity(nops);
        for op in operands {
            if let Ok(k) = deserialize::<Vec<UnaryOps<T>>>(op) {
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
        let (ttl, mut result) = existing_val
            .and_then(|ev| {
                let ttl = Cursor::new(ev).read_i64::<LittleEndian>().ok()?;
                deserialize::<VecDeque<T>>(&ev[HDR_LEN..]).ok().map(
                    |s| (ttl, s),
                )
            })
            .unwrap_or((::std::i64::MAX, VecDeque::<T>::new()));

        println!("({:?})", ttl);
        let ops = operands.flat_map(|op| if let Ok(k) = deserialize::<Vec<UnaryOps<T>>>(op) {
            k.into_iter()
        } else {
            empty.clone().into_iter()
        });

        for op in ops {
            match op {
                UnaryOps::Insert(i, t) => {
                    result.insert(i, t);
                }
                UnaryOps::Replace(i, t) => {
                    result.push_back(t);
                    result.swap_remove_back(i);
                }
                UnaryOps::Del(i) => {
                    result.remove(i);
                }
                UnaryOps::Push(t) => {
                    result.push_back(t);
                }
                _ => debug!("Invalid op supplied to Set: {}", op),
            }
        }

        let mut vbuf: Vec<u8> = Vec::with_capacity(32);
        vbuf.write_i64::<LittleEndian>(ttl)
            .map_err(|e| debug!("failed to serialize ttl: {:?}", e))
            .ok()?;
        serialize_into(&mut vbuf, &result, Infinite)
            .map_err(|e| debug!("failed to serialize result {:?}", e))
            .ok()?;
        Some(vbuf)
    }
}

fn ttl_expired(inbuf: &[u8]) -> Result<bool, RocksCacheError> {
    let ttl = {
        Cursor::new(inbuf).read_i64::<LittleEndian>()?
    };
    Ok(ttl < time::get_time().sec)
}

fn set_ttl(vbuf: &mut Vec<u8>, ttl: Option<i64>) -> Result<(), RocksCacheError> {

    let end = if let Some(t) = ttl {
        time::get_time().sec + t
    } else {
        ::std::i64::MAX
    };
    vbuf.write_i64::<LittleEndian>(end).map_err(
        RocksCacheError::from,
    )
}

pub fn ttl_filter(_level: u32, _key: &[u8], value: &[u8]) -> CompactionDecision {
    use CompactionDecision::*;
    match ttl_expired(value) {
        Ok(true) => Keep,
        Ok(false) => Remove,
        Err(_) => Keep,
    }
}
