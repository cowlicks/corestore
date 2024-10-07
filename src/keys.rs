use hypercore::PartialKeypair;
use hypercore_protocol::DiscoveryKey;

const OUT_SIZE: usize = 32;

// comes from js corestore's
// https://github.com/holepunchto/corestore/blob/1cca652289b3be5bdf3d5da258865e0d3eff6bf6/index.js#L11
// should be constant?
const NS: [u8; 32] = [
    172, 92, 76, 191, 177, 6, 71, 118, 64, 70, 55, 162, 128, 199, 31, 172, 130, 50, 129, 211, 81,
    235, 236, 237, 3, 21, 23, 67, 39, 13, 239, 41,
];

fn namespace(name: &str, count: usize) -> Vec<[u8; 32]> {
    todo!()
}

pub unsafe fn deriveSeed(primary_key: [u8; 32], namespace: &str, name: &str) -> PartialKeypair {
    let mut out = Vec::with_capacity(32);
    let mut input = Vec::new();
    let name_bytes: Vec<u8> = name.into();
    input.extend_from_slice(&NS);
    //input.extend_from_slice(&names);
    input.extend_from_slice(&name_bytes);

    let ret = libsodium_sys::crypto_generichash(
        out.as_mut_ptr(),
        OUT_SIZE,
        input.as_mut_ptr(),
        input.len() as u64,
        primary_key.as_ptr(),
        32,
    );
    dbg!(&ret);
    todo!()
}

#[test]
fn key_from_name() {
    todo!()
}
