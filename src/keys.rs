/*
 * Keys:
 * We a bunch of names for related things:
 * - `hc::PartialKeypair` ({ public: VerifyingKey, secret: SigningKey })
 * - `hc::SigningKey` (from ed25519::SigningKey`)
 * - `hc::VerifyingKey` (from ed25519::VerifyingKey`)
 * - `hc::SecretKey` (from ed25519::SecretKey`) only used in proto
 * - `corestore::PrimaryKey`
 * - `proto::protocol::DiscoveryKey`
 * - `proto::protocol::Key`
 * - `proto::protocol::RemotePublicKey`
 * first notice:
 * ```
 * PartialKeypair {
 *  public: VerifyingKey,
 *  secret: Option<SigningKey>,
 * }
 * ```
 * When `PartialKeypair::secret.is_some()` you can write to a core, when it `is_none()` you are a
 * reader. But use still use the `PartialKeypair` to verify data.
 *
 * also `VerifyingKey` impls `From<&SigningKey>`. So the full `PartialKeypair` can be defined from
 * a `SigningKey`.
 *
 * What is a `PrimaryKey`. It is just 32 cyptograpically random bytes.
 * It is used to derive the `SigningKey` for each new hypercore the corestore has
 *
 * We use `PrimaryKey` to deterministically derive new `PartialKeypair`'s.
 * see [`create_key_pair`]
 *
 * `DiscoveryKey` is derived from `VerifyingKey`:
 * ```
 * hypercore_protocol::discovery_key(verifying_key.as_bytes())
 * ```
 * where it is basically just hashed
 *
 * there is also the `id` of a hypercore that corestore uses as the name of the directory the
 * hypercore data is stored in.
 *
 *
 * What is `Key`?
 * it seems like it is a shared secret that is used to initiate a connection with a remote
 * hypercore.
 * created as [0u8;32] in proto/benches/pipe.rs???
 * it is `DiscoveryKey` bytes in proto/examples/replication.rs
 * So it seems like it is derived arbitrarily.
 * What does js use for this "key"?
 *
 * So keys are all derived from primary key like:
 * ```
 * PrimaryKey (+ name) -> SigningKey -> VerifyingKey & PartialKeypair -> DiscoveryKey
 *
 * TODO we need some methods to make handling keys easier. Probably something like:
 * fn (PrimaryKey, name: &str) -> SigningKey
 * impl Frome<PartialKeypair> for DiscoveryKey
 * impl Frome<SigningKey> for PartialKeypair
 * impl Frome<SigningKey> for VerifyingKey
 * impl Frome<VerifyingKey> for DiscoveryKey
 *
 * Some of these are redundant so implementing all is probably the wrong thing.
 *
*/
use crate::{Error, Namespace, PrimaryKey, Result};
use hypercore::{PartialKeypair, SigningKey, VerifyingKey};
pub const DEFAULT_NAMESPACE: Namespace = [0; 32];
const SEED_SIZE: usize = 32;
const SODIUM_CRYPTO_SIGN_PUBLICKEYBYTES: usize = 32;
const SODIUM_CRYPTO_SIGN_SECRETKEYBYTES: usize = 64;

// comes from js corestore's
// https://github.com/holepunchto/corestore/blob/1cca652289b3be5bdf3d5da258865e0d3eff6bf6/index.js#L11
// should be constant?
const NS: [u8; 32] = [
    172, 92, 76, 191, 177, 6, 71, 118, 64, 70, 55, 162, 128, 199, 31, 172, 130, 50, 129, 211, 81,
    235, 236, 237, 3, 21, 23, 67, 39, 13, 239, 41,
];

unsafe fn derive_seed(
    primary_key: PrimaryKey,
    namespace: &Namespace,
    name: &str,
) -> Result<Vec<u8>> {
    let mut out = vec![0; SEED_SIZE];
    let mut input = Vec::new();
    let name_bytes: Vec<u8> = name.into();
    input.extend_from_slice(&NS);
    input.extend_from_slice(namespace);
    input.extend_from_slice(&name_bytes);

    let ret = libsodium_sys::crypto_generichash(
        out.as_mut_ptr(),
        SEED_SIZE,
        input.as_mut_ptr(),
        input.len() as u64,
        primary_key.as_ptr(),
        32,
    );
    if ret != 0 {
        return Err(Error::LibSodiumGenericHashError(ret));
    }
    Ok(out)
}

pub fn key_pair_from_name(
    primary_key: PrimaryKey,
    namespace: &Namespace,
    name: &str,
) -> Result<PartialKeypair> {
    let mut public_key: Vec<u8> = vec![0; SODIUM_CRYPTO_SIGN_PUBLICKEYBYTES];
    let mut secret_key: Vec<u8> = vec![0; SODIUM_CRYPTO_SIGN_SECRETKEYBYTES];
    unsafe {
        let seed = derive_seed(primary_key, namespace, name)?;
        let ret = libsodium_sys::crypto_sign_seed_keypair(
            public_key.as_mut_ptr(),
            secret_key.as_mut_ptr(),
            seed.as_ptr(),
        );
        if ret != 0 {
            return Err(Error::LibSodiumSignSeedKeypair(ret));
        }
    };
    let signing_key = SigningKey::from_keypair_bytes(&secret_key.try_into().unwrap())?;
    let verifying_key = VerifyingKey::from_bytes(&public_key.try_into().unwrap())?;
    Ok(PartialKeypair {
        public: verifying_key,
        secret: Some(signing_key),
    })
}

#[test]
fn check_derive_seed() -> Result<()> {
    // got this from running js's derive seed
    let expected: [u8; 32] = [
        20, 199, 106, 232, 158, 115, 83, 70, 13, 9, 129, 194, 50, 190, 160, 158, 46, 252, 91, 200,
        138, 10, 110, 49, 141, 167, 190, 36, 160, 145, 113, 106,
    ];
    let name = "foo";
    let primary_key: [u8; 32] = [
        45, 168, 29, 102, 53, 33, 238, 58, 210, 74, 33, 141, 133, 20, 97, 2, 65, 13, 85, 203, 174,
        189, 180, 85, 43, 11, 202, 208, 12, 156, 36, 122,
    ];

    let result = unsafe { derive_seed(primary_key, &DEFAULT_NAMESPACE, name)? };

    assert_eq!(result, expected);
    Ok(())
}
