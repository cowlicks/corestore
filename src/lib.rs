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

mod builder;
mod keys;
mod storage;
use futures_lite::{AsyncRead, AsyncWrite};
use hypercore_protocol::{discovery_key, DiscoveryKey, Event, ProtocolBuilder};
use std::{collections::HashMap, sync::Arc};
use storage::StorageKind;
use tokio::{spawn, sync::RwLock};
use tracing::{debug, error, warn};

use hypercore::{replication::CoreMethodsError, HypercoreError, VerifyingKey};

use replicator::{on_peer, ProtoMethods, ReplicatingCore, ReplicatorError};

pub use builder::{CorestoreBuilder, CorestoreBuilderError};

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

    fn verifying_keys(&self) -> Vec<&VerifyingKey> {
        self.verifying_key_to_cores.keys().collect()
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

/// Replace Corestore with this
#[derive(Debug, Clone)]
pub struct Corestore {
    ///  shared ref to corestore
    corestore: Arc<RwLock<builder::InnerCorstore>>,
}

impl Corestore {
    /// Create a new [`Corestore`] that stores it data in RAM
    pub async fn new_mem() -> Corestore {
        CorestoreBuilder::default()
            .storage(StorageKind::new_mem())
            .build()
            .await
            .expect("should always work")
    }

    /// Get a hypercore by name. If the core does not exist, create it.
    pub async fn get_from_name(&self, name: &str) -> Result<ReplicatingCore> {
        self.corestore.write().await.get_from_name(name).await
    }

    /// Get a core from it's [`VerifyingKey`].
    pub async fn get_from_verifying_key(
        &self,
        verifying_key: &VerifyingKey,
    ) -> Result<ReplicatingCore> {
        self.corestore
            .write()
            .await
            .get_from_verifying_key(verifying_key)
            .await
    }

    /// Start replicating through the given stream
    pub async fn replicate<S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static>(
        &self,
        stream: S,
        is_initiator: bool,
    ) -> Result<()> {
        let protocol = ProtocolBuilder::new(is_initiator).connect(stream);
        let protocol: Arc<RwLock<Box<dyn ProtoMethods>>> =
            Arc::new(RwLock::new(Box::new(protocol)));

        let cs = self.clone();
        spawn(async move {
            while let Some(Ok(event)) = {
                // this block is just here to release the `.write()` lock
                #[allow(clippy::let_and_return)]
                let p = protocol.write().await._next().await;
                p
            } {
                debug!("Protocol event: [{event:?}]");
                match event {
                    Event::Handshake(_m) => {
                        // TODO associate a "name" with this stream and log it.
                        if is_initiator {
                            let vks: Vec<VerifyingKey> = cs
                                .corestore
                                .read()
                                .await
                                .verifying_keys()
                                .into_iter()
                                .cloned()
                                .collect();
                            // TODO spawn this?
                            for vk in vks {
                                protocol.write().await.open(*vk.as_bytes()).await?;
                            }
                        }
                        debug!("Handshake complete. Session secured")
                    }
                    Event::DiscoveryKey(dk) => {
                        if let Some(vk) = cs
                            .corestore
                            .read()
                            .await
                            .verifying_key_from_discovery_key(&dk)
                        {
                            protocol.write().await.open(*vk.as_bytes()).await?;
                        }
                    }
                    Event::Channel(channel) => {
                        // this channel is only opened after protocol.open(..) verifies we have the
                        // same pub key.. Correct?
                        //
                        // get the core associated with this channel's dk.
                        let Some(vk) = cs
                            .corestore
                            .read()
                            .await
                            .verifying_key_from_discovery_key(&channel.discovery_key())
                        else {
                            panic!(
                            "We **should** have verified that we have a core with this &dk already"
                        );
                        };
                        // get core replicating over the channel...
                        let core = cs
                            .corestore
                            .write()
                            .await
                            .get_from_verifying_key(&vk)
                            .await?;
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
            Ok::<(), Error>(())
        });
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use hypercore::replication::{CoreInfo, CoreMethods};
    use replicator::utils::create_connected_streams;
    use tokio::time::sleep;

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

        let cs = CorestoreBuilder::default()
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
            let cs = CorestoreBuilder::default()
                .primary_key(pk)
                .storage(StorageKind::new_disk(storage_dir.path()))
                .build()
                .await?;

            let hc = cs.get_from_name("foo").await?;
            hc.append(b"hello").await?;
        }
        {
            // corestore uses pk in directory if it exists already
            let cs = CorestoreBuilder::default()
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
    async fn prexisting_cores_replicate() -> Result<()> {
        let (cs_a, cs_b) = (Corestore::new_mem().await, Corestore::new_mem().await);
        let (a, b) = create_connected_streams();
        let name = "foo";
        let core_a = cs_a.get_from_name(name).await?;
        let pk = core_a.key_pair().await.public.clone();
        let core_b = cs_b.get_from_verifying_key(&pk).await?;

        core_a.append(b"hello").await?;
        assert!(core_b.get(0).await?.is_none());

        cs_a.replicate(a, false).await?;
        cs_b.replicate(b, true).await?;
        loop {
            if core_b.get(0).await?.is_some() {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn new_cores_replicate() -> Result<()> {
        let (cs_a, cs_b) = (Corestore::new_mem().await, Corestore::new_mem().await);
        let (a, b) = create_connected_streams();

        utils::log();
        cs_a.replicate(a, false).await?;
        cs_b.replicate(b, true).await?;

        sleep(Duration::from_millis(25)).await;

        let name = "foo";
        let core_a = cs_a.get_from_name(name).await?;
        dbg!(core_a.append(b"hello").await?);

        let pk = core_a.key_pair().await.public.clone();
        let core_b = cs_b.get_from_verifying_key(&pk).await?;

        core_a.append(b"world").await?;
        loop {
            if let Some(x) = core_b.get(0).await? {
                assert_eq!(x, b"hello");
                break;
            }
            dbg!();
            sleep(Duration::from_millis(25)).await;
        }
        loop {
            if let Some(x) = core_b.get(1).await? {
                assert_eq!(x, b"world");
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }
}
