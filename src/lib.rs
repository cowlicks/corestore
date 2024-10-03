/*!
TODO Crate level docs
```rust
# use rust::add;
assert_eq!(add(1, 2), 3);
```
*/
#![warn(
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    non_local_definitions,
    unsafe_code,
    non_local_definitions
)]

use hypercore::Hypercore;
use random_access_storage::RandomAccess;

#[derive(thiserror::Error, Debug)]
pub enum Error {}

type Result<T> = std::result::Result<T, Error>;

struct Corestore {
    storage: Box<dyn RandomAccess>,
}

impl Corestore {
    fn new(storage: Box<dyn RandomAccess>) -> Self {
        Self { storage }
    }

    fn get_name(name: &str) -> Result<Hypercore> {
        todo!()
    }
}
