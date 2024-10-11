//!
//! [`Corestore`] provides a way to manage a related group of [`SharedCore`]s.
//! Intended to be fully compatible with the [JavaScrpt `corestore`
//! library](https://github.com/holepunchto/corestore).
#![warn(
    missing_debug_implementations,
    //missing_docs,
    redundant_lifetimes,
    non_local_definitions,
    //unsafe_code,
    non_local_definitions
)]
#![allow(unused)]
#![allow(unused_variables)]

pub mod keys;
use keys::{key_pair_from_name, verifying_key_from_name, DEFAULT_NAMESPACE};
use rand::{rngs::OsRng, RngCore};
use std::{
    collections::{BTreeMap, HashMap},
    env::join_paths,
    fs::{self, read, write, OpenOptions},
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
#[non_exhaustive]
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
    #[error("Fs error")]
    BuilderError(#[from] CorestoreBuilderError),
    #[error("Could not build corestore because a primary key value was provided, but one already exists on disk at [{0}]")]
    PrimaryKeyConflict(String),
}

type Result<T> = std::result::Result<T, Error>;

fn generate_primary_key() -> PrimaryKey {
    let mut csprng = OsRng;
    let mut primary_key = [0u8; 32];
    csprng.fill_bytes(&mut primary_key);
    primary_key
}

#[derive(Debug)]
pub enum StorageKind {
    Mem,
    Disk(PathBuf),
}

impl StorageKind {
    pub fn new_disk(prefix: impl AsRef<Path>) -> Self {
        let path = prefix.as_ref().to_path_buf();
        Self::Disk(prefix.as_ref().to_path_buf())
    }

    pub async fn get_core_from_key_pair(
        &self,
        primary_key: PrimaryKey,
        kp: PartialKeypair,
    ) -> Result<SharedCore> {
        match self {
            StorageKind::Mem => {
                let s = Storage::new_memory().await?;
                let hc = HypercoreBuilder::new(s).key_pair(kp).build().await?;
                Ok(SharedCore::from(hc))
            }
            StorageKind::Disk(path) => {
                let path_to_storage = get_storage_root(&kp);
                let full_path = path.join(path_to_storage);
                let s = Storage::new_disk(&full_path, false).await?;
                let hc = HypercoreBuilder::new(s).key_pair(kp).build().await?;
                Ok(SharedCore::from(hc))
            }
        }
    }

    pub async fn get_core_from_dk(
        &self,
        verifying_key: &VerifyingKey,
    ) -> Result<Option<SharedCore>> {
        match self {
            StorageKind::Mem => Ok(None),
            StorageKind::Disk(path) => {
                let path_to_storage: PathBuf = get_storage_root(verifying_key).into();
                let full_path = path.join(path_to_storage);
                if full_path.exists() {
                    todo!()
                }
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Default)]
struct CoreCache {
    verifying_key_to_cores: HashMap<VerifyingKey, SharedCore>,
}

impl CoreCache {
    // get the dk from a name
    fn insert_by_verifying_key(
        &mut self,
        verifying_key: &VerifyingKey,
        core: SharedCore,
    ) -> Option<SharedCore> {
        self.verifying_key_to_cores.insert(*verifying_key, core)
    }

    fn get_by_verifying_key(&self, verifying_key: &VerifyingKey) -> Option<SharedCore> {
        self.verifying_key_to_cores.get(verifying_key).cloned()
    }
}

#[derive(Debug)]
enum Role {
    Writer,
    Reader,
}

struct StorageId(String);

impl From<&VerifyingKey> for StorageId {
    fn from(value: &VerifyingKey) -> Self {
        StorageId(data_encoding::HEXLOWER.encode(value.as_bytes()))
    }
}

impl From<&PartialKeypair> for StorageId {
    fn from(value: &PartialKeypair) -> Self {
        StorageId(data_encoding::HEXLOWER.encode(&value.public.to_bytes()))
    }
}
fn get_storage_root<T: Into<StorageId>>(to_id: T) -> PathBuf {
    let id: StorageId = to_id.into();

    format!(
        "{CORES_DIR_NAME}/{}/{}/{}",
        id.0[0..2].to_string(),
        id.0[2..4].to_string(),
        id.0
    )
    .into()
}

// TODO add primary_key here. Get it from the cores dir or create it
#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip), derive(Debug))]
pub struct Corestore {
    primary_key: PrimaryKey,
    storage: StorageKind,
    #[builder(default = "Default::default()")]
    core_cache: CoreCache,
    role: Role,
}

impl CorestoreBuilder {
    fn build(self) -> std::result::Result<Corestore, Error> {
        dbg!();
        let Some(role) = self.role else {
            return Err(CorestoreBuilderError::UninitializedField("role").into());
        };

        let Some(storage) = self.storage else {
            return Err(CorestoreBuilderError::UninitializedField("storage").into());
        };
        // Somewhat complicated primary key logic
        dbg!();
        let primary_key: PrimaryKey = match role {
            Role::Reader => todo!(),
            Role::Writer => match storage {
                StorageKind::Mem => generate_primary_key(),
                StorageKind::Disk(ref path) => {
                    let pk_path = path.join(PRIMARY_KEY_FILE_NAME);
                    // key file exists on disk
                    if pk_path.exists() {
                        if self.primary_key.is_some() {
                            return Err(Error::PrimaryKeyConflict(pk_path.display().to_string()));
                        }
                        std::fs::read(pk_path)?
                            .try_into()
                            // Fail if key on disk is invalid
                            .map_err(|_| Error::InvalidPrimaryKey)?
                    } else {
                        // No existing key on disk, create one
                        let pk = generate_primary_key();
                        write(pk_path, &pk);
                        pk
                    }
                }
            },
        };
        Ok(Corestore {
            primary_key,
            storage,
            core_cache: self.core_cache.unwrap_or_default(),
            role,
        })
    }
}

impl Corestore {
    /// Get a hypercore by name. If the core does not exist, create it.
    pub async fn get_from_name(&mut self, name: &str) -> Result<SharedCore> {
        let kp = key_pair_from_name(self.primary_key, &DEFAULT_NAMESPACE, name)?;

        if let Some(core) = self.core_cache.get_by_verifying_key(&kp.public) {
            return Ok(core);
        };
        let core = self
            .storage
            .get_core_from_key_pair(self.primary_key, kp.clone())
            .await?;
        self.core_cache
            .insert_by_verifying_key(&kp.public, core.clone());
        Ok(core)
    }

    pub async fn get_from_verifying_key(
        &self,
        verifying_key: &VerifyingKey,
    ) -> Result<Option<SharedCore>> {
        if let Some(core) = self.core_cache.get_by_verifying_key(&verifying_key) {
            return Ok(Some(core));
        };
        match &self.storage {
            StorageKind::Mem => Ok(None),
            StorageKind::Disk(path) => {
                let core_path = path.join(get_storage_root(verifying_key));
                if core_path.exists() {}
                todo!()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use hypercore::{generate_signing_key, replication::CoreMethods};

    use super::*;

    const TEST_PK: PrimaryKey = [
        124, 229, 174, 223, 232, 201, 160, 10, 235, 143, 37, 249, 107, 92, 35, 125, 68, 246, 2,
        197, 41, 248, 234, 65, 9, 222, 77, 144, 50, 243, 222, 65,
    ];

    #[test]
    fn get_storage_dir_matches_js() {
        let id = StorageId("helloworld".to_string());
        assert_eq!(get_storage_root(id).as_os_str(), "cores/he/ll/helloworld");
    }

    #[tokio::test]
    async fn disk_core_by_name() -> Result<()> {
        // initialize CS with a fixed primary key
        // check it producets the expected fixed file path
        let storage_dir = tempfile::tempdir().unwrap();
        let mut pk = TEST_PK.clone();
        pk[0] = 0;

        let mut cs = CorestoreBuilder::default()
            .primary_key(pk)
            .role(Role::Writer)
            .storage(StorageKind::new_disk(storage_dir.path()))
            .build()?;

        let hc = cs.get_from_name("foo").await?;
        hc.append(b"hello").await?;
        assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        let vk = hc.key_pair().await.public;

        let core_path = storage_dir.path().join(get_storage_root(&vk));
        assert!(core_path.exists());

        let hc2 = cs.get_from_verifying_key(&vk).await?.unwrap();
        assert_eq!(hc2.get(0).await?, Some(b"hello".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn primary_key_cstore_dir_gets_used() -> Result<()> {
        let storage_dir = tempfile::tempdir().unwrap();
        let mut pk = TEST_PK.clone();
        pk[0] = 1;

        {
            let mut cs = CorestoreBuilder::default()
                .role(Role::Writer)
                .primary_key(pk)
                .storage(StorageKind::new_disk(storage_dir.path()))
                .build()?;

            let hc = cs.get_from_name("foo").await?;
            hc.append(b"hello").await?;
        }
        {
            // corestore uses pk in directory if it exists already
            let mut cs = CorestoreBuilder::default()
                .storage(StorageKind::new_disk(storage_dir.path()))
                .role(Role::Writer)
                .build()?;
            let hc = cs.get_from_name("foo").await?;
            assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        }
        {
            // providing a pk while there is one on disk is err
            assert!(matches!(
                CorestoreBuilder::default()
                    .storage(StorageKind::new_disk(storage_dir.path()))
                    .primary_key(pk)
                    .role(Role::Writer)
                    .build(),
                Err(Error::PrimaryKeyConflict(_))
            ));
        }

        Ok(())
    }
}
