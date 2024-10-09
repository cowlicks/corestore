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
    //unsafe_code,
    non_local_definitions
)]

pub mod keys;
use rand::{rngs::OsRng, RngCore};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use hypercore::{
    generate_signing_key,
    replication::{CoreInfo, CoreMethodsError, SharedCore},
    HypercoreBuilder, HypercoreError, PartialKeypair, Storage, VerifyingKey,
};
// TODO this is just a type alias. If it's all we need from hc proto, then we should drop hc proto
// as a dependency
use hypercore_protocol::{discovery_key, DiscoveryKey};
use random_access_memory::RandomAccessMemory;

const CORES_DIR_NAME: &str = "cores";
const PRIMARY_KEY_FILE_NAME: &str = "primary-key";

type PrimaryKey = [u8; 32];
type Namespace = [u8; 32];

/// Corestore's Errors
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error from hypercore: {0}")]
    Hypercore(#[from] HypercoreError),
    #[error("error from hypercore CoreMethods: {0}")]
    CoreMethods(#[from] CoreMethodsError),
    #[error("Signature error")]
    Signature(#[from] signature::Error),
    #[error("Fs error")]
    FsError(#[from] std::io::Error),
    #[error("Invalid primary key")]
    InvalidPrimaryKey,
}

type Result<T> = std::result::Result<T, Error>;

fn new_primary_key() -> PrimaryKey {
    let mut csprng = OsRng;
    let mut primary_key = [0u8; 32];
    csprng.fill_bytes(&mut primary_key);
    primary_key
}

fn get_or_create_primary_key(path: impl AsRef<Path>) -> Result<PrimaryKey> {
    if !path.as_ref().exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let primary_key = new_primary_key();
        file.write_all(&primary_key)?;
    }
    Ok(fs::read(&path)?
        .try_into()
        .map_err(|_| Error::InvalidPrimaryKey)?)
}

#[derive(Debug)]
pub enum StorageKind {
    Mem,
    Disk(PathBuf),
}

impl StorageKind {
    pub fn new_disk(prefix: impl AsRef<Path>) -> Self {
        let path = prefix.as_ref().to_path_buf();
        //let std::fs::read(path)?;
        Self::Disk(prefix.as_ref().to_path_buf())
    }

    pub async fn get_from_name(&self, name: &str) -> Result<SharedCore> {
        match self {
            StorageKind::Mem => {
                // TODO is there a way to re-use the ram here?
                // Do I need that? Sholud I add impl From<&Ram> for Storage
                let s = Storage::new_memory().await?;
                let hc = HypercoreBuilder::new(s).build().await?;
                Ok(SharedCore::from(hc))
            }
            StorageKind::Disk(path) => {
                //let keypair = keys::create_key_pair(
                let full_path = path.join(name);
                let s = Storage::new_disk(&full_path, false).await?;
                let hc = HypercoreBuilder::new(s).build().await?;
                Ok(SharedCore::from(hc))
            }
        }
    }
}
#[derive(Debug, Default)]
struct CoreCache {
    dk_to_cores: BTreeMap<DiscoveryKey, SharedCore>,
}

fn id_from_name(name: &str, primary_key: PrimaryKey) -> DiscoveryKey {
    todo!()
}

impl CoreCache {
    // get the dk from a name
    fn insert_by_dk(&mut self, dk: DiscoveryKey, core: SharedCore) -> Option<SharedCore> {
        self.dk_to_cores.insert(dk, core)
    }

    fn get_by_dk(&mut self, dk: DiscoveryKey, core: SharedCore) -> Option<SharedCore> {
        self.dk_to_cores.get(&dk).cloned()
    }

    fn insert_by_name(&mut self, name: &str, core: SharedCore) -> Option<SharedCore> {
        todo!()
        //self.dk_to_cores.insert(dk, core)
    }

    fn get_by_name(&self, name: &str) -> Option<SharedCore> {
        todo!()
        //self.dk_to_cores.get(dk).cloned()
    }
}

// TODO add primary_key here. Get it from the cores dir or create it
#[derive(Debug)]
pub struct Corestore {
    primary_key: PrimaryKey,
    storage: StorageKind,
    core_cache: CoreCache,
}

fn id_from_dk(dk: &DiscoveryKey) -> String {
    data_encoding::HEXLOWER.encode(dk)
}

fn get_storage_root(id: &str) -> String {
    format!(
        "{CORES_DIR_NAME}/{}/{}/{id}",
        id[0..2].to_string(),
        id[2..4].to_string()
    )
}

impl Corestore {
    pub fn new(storage: StorageKind) -> Result<Self> {
        let primary_key = match &storage {
            StorageKind::Mem => new_primary_key(),
            StorageKind::Disk(path) => get_or_create_primary_key(path)?,
        };
        Ok(Self {
            primary_key,
            storage,
            core_cache: Default::default(),
        })
    }

    /// Get a hypercore by name. If the core does not exist, create it.
    pub async fn get_from_name(&mut self, name: &str) -> Result<SharedCore> {
        if let Some(core) = self.core_cache.get_by_name(name) {
            return Ok(core);
        };
        // gets or create core
        let core = self.storage.get_from_name(name).await?;
        // insert it by dk into the cache
        //let dk = discovery_key(&core.key_pair().await.public.to_bytes()); self.core_cache.insert_name_and_dk(name, dk, core.clone());
        Ok(core)
    }

    /// Get a hypercore by
    pub async fn get_from_discover_key(
        &mut self,
        key: &DiscoveryKey,
    ) -> Result<Option<SharedCore>> {
        //if let Some(core) = self.core_cache.get_dk(key) {
        //    return Ok(Some(core));
        //};
        todo!()
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
        let mut cs = Corestore::new(StorageKind::Mem)?;
        let hc = cs.get_from_name("foo").await?;
        hc.append(b"hello").await?;
        assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn get_name_disk() -> Result<()> {
        let storage_dir = tempfile::tempdir().unwrap();
        let s = StorageKind::new_disk(storage_dir.path());
        let mut cs = Corestore::new(s)?;
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
        let mut cs = Corestore::new(s)?;
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
        let mut cs = Corestore::new(StorageKind::Mem)?;
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
        let mut cs = Corestore::new(s)?;
        let hc1 = cs.get_from_name("foo").await?;
        let hc2 = cs.get_from_name("foo").await?;
        hc1.append(b"hello").await?;
        assert_eq!(hc1.get(0).await?, Some(b"hello".to_vec()));
        assert_eq!(hc2.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }
}
