//!
//! [`Corestore`] provides a way to manage a related group of [`SharedCore`]s.
//! Intended to be fully compatible with the [JavaScrpt `corestore`
//! library](https://github.com/holepunchto/corestore).
//!
#![warn(
    missing_debug_implementations,
    //missing_docs,
    redundant_lifetimes,
    non_local_definitions,
    unsafe_code,
    non_local_definitions
)]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use hypercore::{
    replication::{CoreInfo, CoreMethodsError, SharedCore},
    HypercoreBuilder, HypercoreError, Storage,
};
// TODO this is just a type alias. If it's all we need from hc proto, then we should drop hc proto
// as a dependency
use hypercore_protocol::{discovery_key, DiscoveryKey};
use random_access_memory::RandomAccessMemory;

const CORES_DIR: &str = "cores";
const _PRIMARY_KEY_FILE: &str = "primary-key";

/// Corestore's Errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error from hypercore: {0}")]
    Hypercore(#[from] HypercoreError),
    #[error("error from hypercore CoreMethods: {0}")]
    CoreMethods(#[from] CoreMethodsError),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum StorageKind {
    Mem(RandomAccessMemory),
    Disk(PathBuf),
}

impl StorageKind {
    pub fn new_disk(prefix: impl AsRef<Path>) -> Self {
        Self::Disk(prefix.as_ref().to_path_buf())
    }

    pub async fn get_from_name(&self, name: &str) -> Result<SharedCore> {
        match self {
            StorageKind::Mem(_) => {
                // TODO is there a way to re-use the ram here?
                // Do I need that? Sholud I add impl From<&Ram> for Storage
                let s = Storage::new_memory().await?;
                let hc = HypercoreBuilder::new(s).build().await?;
                Ok(SharedCore::from(hc))
            }
            StorageKind::Disk(path) => {
                let full_path = path.join(name);
                let s = Storage::new_disk(&full_path, false).await?;
                let hc = HypercoreBuilder::new(s).build().await?;
                Ok(SharedCore::from(hc))
            }
        }
    }
}
impl From<RandomAccessMemory> for StorageKind {
    fn from(value: RandomAccessMemory) -> Self {
        Self::Mem(value)
    }
}

#[derive(Debug, Default)]
struct CoreCache {
    dk_to_cores: BTreeMap<DiscoveryKey, SharedCore>,
    name_to_dk: BTreeMap<String, DiscoveryKey>,
}

impl CoreCache {
    fn insert_by_dk(&mut self, dk: DiscoveryKey, core: SharedCore) -> Option<SharedCore> {
        self.dk_to_cores.insert(dk, core)
    }

    fn insert_name_and_dk(
        &mut self,
        name: &str,
        dk: DiscoveryKey,
        core: SharedCore,
    ) -> (Option<DiscoveryKey>, Option<SharedCore>) {
        (
            self.name_to_dk.insert(name.to_string(), dk),
            self.insert_by_dk(dk, core),
        )
    }

    fn get_dk(&self, dk: &DiscoveryKey) -> Option<SharedCore> {
        self.dk_to_cores.get(dk).cloned()
    }

    fn get_name(&self, name: &str) -> Option<SharedCore> {
        self.get_dk(self.name_to_dk.get(name)?)
    }
}

#[derive(Debug)]
pub struct Corestore {
    storage: StorageKind,
    core_cache: CoreCache,
}

fn id_from_dk(dk: &DiscoveryKey) -> String {
    data_encoding::HEXLOWER.encode(dk)
}

fn get_storage_root(id: &str) -> String {
    format!(
        "{CORES_DIR}/{}/{}/{id}",
        id[0..2].to_string(),
        id[2..4].to_string()
    )
}

impl Corestore {
    pub fn new(storage: StorageKind) -> Self {
        Self {
            storage,
            core_cache: Default::default(),
        }
    }

    /// Get a hypercore by name. If the core does not exist, create it.
    pub async fn get_from_name(&mut self, name: &str) -> Result<SharedCore> {
        if let Some(core) = self.core_cache.get_name(name) {
            return Ok(core);
        };
        let core = self.storage.get_from_name(name).await?;
        let dk = discovery_key(&core.key_pair().await.public.to_bytes());
        self.core_cache.insert_name_and_dk(name, dk, core.clone());
        Ok(core)
    }

    /// Get a hypercore by name. If the core does not exist, create it.
    pub async fn get_from_discover_key(
        &mut self,
        key: &DiscoveryKey,
    ) -> Result<Option<SharedCore>> {
        if let Some(core) = self.core_cache.get_dk(key) {
            return Ok(Some(core));
        };
        let id = id_from_dk(key);
        let name = get_storage_root(&id);
        let core = self.storage.get_from_name(&name).await?;
        self.core_cache
            .insert_name_and_dk(&name, *key, core.clone());
        Ok(Some(core))
    }
}

#[cfg(test)]
mod test {
    use hypercore::{generate_signing_key, replication::CoreMethods};

    use super::*;

    #[test]
    fn get_storage_dir_matches_js() {
        let id = "helloworld";
        assert_eq!(get_storage_root(id), "cores/he/ll/helloworld");
    }

    #[tokio::test]
    async fn get_name_mem() -> Result<()> {
        let mut cs = Corestore::new(RandomAccessMemory::default().into());
        let hc = cs.get_from_name("foo").await?;
        hc.append(b"hello").await?;
        assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn get_name_disk() -> Result<()> {
        let storage_dir = tempfile::tempdir().unwrap();
        let s = StorageKind::new_disk(storage_dir.path());
        let mut cs = Corestore::new(s);
        {
            let hc = cs.get_from_name("foo").await?;
            hc.append(b"hello").await?;
            assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        }
        {
            let hc = cs.get_from_name("foo").await?;
            assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_core_cache_disk() -> Result<()> {
        let storage_dir = tempfile::tempdir().unwrap();
        let s = StorageKind::new_disk(storage_dir.path());
        let mut cs = Corestore::new(s);
        let hc1 = cs.get_from_name("foo").await?;
        let hc2 = cs.get_from_name("foo").await?;
        hc1.append(b"hello").await?;
        assert_eq!(hc1.get(0).await?, Some(b"hello".to_vec()));
        assert_eq!(hc2.get(0).await?, Some(b"hello".to_vec()));

        let dk = discovery_key(hc1.key_pair().await.public.as_bytes());
        let hc3 = cs.get_from_discover_key(&dk).await?.unwrap();
        assert_eq!(hc3.get(0).await?, Some(b"hello".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn core_cache_mem() -> Result<()> {
        let mut cs = Corestore::new(RandomAccessMemory::default().into());
        let hc1 = cs.get_from_name("foo").await?;
        let hc2 = cs.get_from_name("foo").await?;
        hc1.append(b"hello").await?;
        assert_eq!(hc1.get(0).await?, Some(b"hello".to_vec()));
        assert_eq!(hc2.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn disk_name() -> Result<()> {
        let storage_dir = "bar";
        let s = StorageKind::new_disk(PathBuf::from(storage_dir));
        let mut cs = Corestore::new(s);
        let hc1 = cs.get_from_name("foo").await?;
        let hc2 = cs.get_from_name("foo").await?;
        hc1.append(b"hello").await?;
        assert_eq!(hc1.get(0).await?, Some(b"hello".to_vec()));
        assert_eq!(hc2.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }
}
