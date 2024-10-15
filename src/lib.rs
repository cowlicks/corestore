//! [`Corestore`] provides a way to manage a related group of [`Hypercore`]s.
//! Intended to be fully compatible with the [JavaScrpt `corestore`
//! library](https://github.com/holepunchto/corestore).
#![warn(
    missing_debug_implementations,
    missing_docs,
    redundant_lifetimes,
    non_local_definitions,
    //unsafe_code,
    non_local_definitions
)]

mod keys;
mod storage;
use futures_lite::{AsyncRead, AsyncWrite};
use hypercore_protocol::{discovery_key, DiscoveryKey, Event, ProtocolBuilder};
use keys::{key_pair_from_name, DEFAULT_NAMESPACE};
use std::{collections::HashMap, sync::Arc};
use storage::StorageKind;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use hypercore::{
    replication::{CoreInfo, CoreMethodsError},
    HypercoreError, PartialKeypair, VerifyingKey,
};

use replicator::{on_peer, ProtoMethods, ReplicatingCore, ReplicatorError};

const CORES_DIR_NAME: &str = "cores";
const PRIMARY_KEY_FILE_NAME: &str = "primary-key";

type PrimaryKey = [u8; 32];
type Namespace = [u8; 32];

/// Corestore's Errors
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[error("error from hypercore: {0}")]
    Hypercore(#[from] HypercoreError),
    #[error("error from ReplicatingCore: {0}")]
    ReplicatingCore(#[from] ReplicatorError),
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
    #[error("libsodium's generichash function did not return `0`. Got: {0}")]
    LibSodiumGenericHashError(i32),
    #[error("libsodium's sign_seed_keypair function did not return `0`. Got: {0}")]
    LibSodiumSignSeedKeypair(i32),
    #[error("error reading dirs {0}")]
    ReadDirError(std::io::Error),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Default)]
struct CoreCache {
    verifying_key_to_cores: HashMap<VerifyingKey, ReplicatingCore>,
}

impl CoreCache {
    // get the dk from a name
    fn insert(
        &mut self,
        verifying_key: &VerifyingKey,
        core: ReplicatingCore,
    ) -> Option<ReplicatingCore> {
        self.verifying_key_to_cores.insert(*verifying_key, core)
    }

    fn get(&self, verifying_key: &VerifyingKey) -> Option<ReplicatingCore> {
        self.verifying_key_to_cores.get(verifying_key).cloned()
    }

    /// TODO make this O(1) by storing a dk -> vk map
    fn verifying_key_from_discovery_key(&self, dk: &DiscoveryKey) -> Option<VerifyingKey> {
        for vk in self.verifying_key_to_cores.keys() {
            if dk == &discovery_key(vk.as_bytes()) {
                return Some(vk.clone());
            }
        }
        None
    }
}

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(skip), derive(Debug))]
/// [`Corestore`] is used to manage a collection of related [`Hypercore`]s.
pub struct Corestore {
    /// The [`PrimaryKey`] used to deterministically derive keys for cores owned by this
    /// `Corestore`
    primary_key: PrimaryKey,
    /// The kind of storage that [`Corestore`] will use to store it's data.
    storage: StorageKind,
    #[builder(default = "Default::default()")]
    /// The place we keep active cores
    core_cache: CoreCache,
}

impl CorestoreBuilder {
    /// Build the [`Corestore`]
    pub async fn build(self) -> std::result::Result<Corestore, Error> {
        let Some(storage) = self.storage else {
            return Err(CorestoreBuilderError::UninitializedField("storage").into());
        };
        // Somewhat complicated primary key logic
        let primary_key: PrimaryKey = storage.get_or_create_primary_key(&self.primary_key)?;
        let mut cs = Corestore {
            primary_key,
            storage,
            core_cache: self.core_cache.unwrap_or_default(),
        };
        for existing_core in cs.storage.load_existing_cores().await? {
            let vk = existing_core.key_pair().await.public;
            cs.add_core(vk, existing_core);
        }
        Ok(cs)
    }
}

impl Corestore {
    /// Create a new [`Corestore`] that stores it data in RAM
    pub async fn new_mem() -> Self {
        CorestoreBuilder::default()
            .storage(StorageKind::new_mem())
            .build()
            .await
            .expect("should always work")
    }

    fn add_core(&mut self, vk: VerifyingKey, core: ReplicatingCore) {
        self.core_cache.insert(&vk, core.clone());
    }

    /// Get a hypercore by name. If the core does not exist, create it.
    /// This does... not? work if there is no verifying key.
    /// Or, maybe, all cores get a primary key, but only writable cores use this?
    /// This would imply that a corestore instance could have mixed readable and writable keys...
    pub async fn get_from_name(&mut self, name: &str) -> Result<ReplicatingCore> {
        let kp = key_pair_from_name(self.primary_key, &DEFAULT_NAMESPACE, name)?;

        if let Some(core) = self.core_cache.get(&kp.public) {
            return Ok(core);
        };
        let core = self.storage.get_core_from_key_pair(kp.clone()).await?;
        self.core_cache.insert(&kp.public, core.clone());
        Ok(core)
    }

    /// Get a core from it's [`VerifyingKey`].
    /// Since the core only has a verifyin key (and no [`SigningKey`]). It is read-only.
    pub async fn get_from_verifying_key(
        &mut self,
        verifying_key: &VerifyingKey,
    ) -> Result<ReplicatingCore> {
        if let Some(core) = self.core_cache.get(&verifying_key) {
            return Ok(core);
        };
        let kp = PartialKeypair {
            public: *verifying_key,
            secret: None,
        };
        let core = self.storage.get_core_from_key_pair(kp.clone()).await?;
        self.core_cache.insert(&kp.public, core.clone());
        Ok(core)
    }

    /// Start replicating through the given stream
    pub async fn replicate<S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static>(
        &mut self,
        stream: S,
        is_initiator: bool,
    ) -> Result<()> {
        let protocol = ProtocolBuilder::new(is_initiator).connect(stream);
        let protocol: Arc<RwLock<Box<dyn ProtoMethods>>> =
            Arc::new(RwLock::new(Box::new(protocol)));
        while let Some(Ok(event)) = {
            // this block is just here to release the `.write()` lock
            #[allow(clippy::let_and_return)]
            let p = protocol.write().await._next().await;
            p
        } {
            match event {
                Event::Handshake(_m) => {
                    // TODO associate a "name" with this stream and log it.
                    debug!("Handshake complete. Session secured")
                }
                Event::DiscoveryKey(dk) => {
                    if let Some(vk) = self.core_cache.verifying_key_from_discovery_key(&dk) {
                        protocol.write().await.open(*vk.as_bytes()).await?;
                    }
                }
                Event::Channel(channel) => {
                    // this channel is only opened after protocol.open(..) verifies we have the
                    // same pub key.. Correct?
                    //
                    // get the core associated with this channel's dk.
                    let Some(vk) = self
                        .core_cache
                        .verifying_key_from_discovery_key(&channel.discovery_key())
                    else {
                        panic!(
                            "We **should** have verified that we have a core with this &dk already"
                        );
                    };
                    // get core replicating over the channel...
                    let core = self.get_from_verifying_key(&vk).await?;
                    // pass channel to peers to replicate
                    let _ = core
                        .add_peer(
                            core.core.clone(),
                            protocol.clone() as Arc<RwLock<Box<dyn ProtoMethods>>>,
                        )
                        .await;
                    on_peer(core.core.clone(), channel).await?;
                }
                Event::Close(_dkey) => {}
                _ => todo!(),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use hypercore::replication::{CoreInfo, CoreMethods};
    use replicator::utils::create_connected_streams;

    use super::{storage::get_storage_root, *};

    const TEST_PK: PrimaryKey = [
        124, 229, 174, 223, 232, 201, 160, 10, 235, 143, 37, 249, 107, 92, 35, 125, 68, 246, 2,
        197, 41, 248, 234, 65, 9, 222, 77, 144, 50, 243, 222, 65,
    ];

    #[tokio::test]
    async fn disk_core_by_name() -> Result<()> {
        // initialize CS with a fixed primary key
        // check it producets the expected fixed file path
        let storage_dir = tempfile::tempdir().unwrap();
        let mut pk = TEST_PK.clone();
        pk[0] = 0;

        let mut cs = CorestoreBuilder::default()
            .primary_key(pk)
            .storage(StorageKind::new_disk(storage_dir.path()))
            .build()
            .await?;

        let hc = cs.get_from_name("foo").await?;
        hc.append(b"hello").await?;
        assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        let vk = hc.key_pair().await.public;

        let core_path = storage_dir.path().join(get_storage_root(&vk));
        assert!(core_path.exists());

        let hc2 = cs.get_from_verifying_key(&vk).await?;
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
                .primary_key(pk)
                .storage(StorageKind::new_disk(storage_dir.path()))
                .build()
                .await?;

            let hc = cs.get_from_name("foo").await?;
            hc.append(b"hello").await?;
        }
        {
            // corestore uses pk in directory if it exists already
            let mut cs = CorestoreBuilder::default()
                .storage(StorageKind::new_disk(storage_dir.path()))
                .build()
                .await?;
            let hc = cs.get_from_name("foo").await?;
            assert_eq!(hc.get(0).await?, Some(b"hello".to_vec()));
        }
        {
            // providing a pk while there is one on disk is err
            assert!(matches!(
                CorestoreBuilder::default()
                    .storage(StorageKind::new_disk(storage_dir.path()))
                    .primary_key(pk)
                    .build()
                    .await,
                Err(Error::PrimaryKeyConflict(_))
            ));
        }

        Ok(())
    }

    #[tokio::test]
    async fn replication() -> Result<()> {
        let (mut cs_a, mut cs_b) = (Corestore::new_mem().await, Corestore::new_mem().await);
        let (a, b) = create_connected_streams();

        let name = "foo";
        let core_a = cs_a.get_from_name(name).await?;
        let pk = core_a.key_pair().await.public.clone();
        let core_b = cs_b.get_from_verifying_key(&pk).await?;

        core_a.append(b"hello").await?;
        assert!(core_b.get(0).await?.is_none());

        cs_a.replicate(a, false).await?;
        cs_b.replicate(b, true).await?;
        // a creates a core by name: foo_core= a.get_name('foo');
        // b gets a core with foo_core's pub_key
        todo!()
    }
}
