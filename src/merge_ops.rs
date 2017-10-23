
#[macro_use]
extern crate serde_derive;

use serde::{Deserialize, Serialize};

pub mod collection {

#[derive(Serialize, Deserialize)]
enum MergeOps<K : Deserialize + Serialize, V: Deserialize + Serialize>  {
    Put(K, V),
    Get(K),
    Add(K),
    Del(K),
    Update(K, V),
    Push(V),
    Pop
}

}
