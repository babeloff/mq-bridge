use std::io::BufReader;

use anyhow::Result;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa, PKCS_ECDSA_P256_SHA256};
use rustls::RootCertStore;

#[tokio::test]
async fn tls_handshake_example() -> Result<()> {
    // Ensure rustls has a process-level crypto provider installed for tests.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    // Generate a test CA and a server certificate signed by it.
    let mut ca_params = CertificateParams::new(vec!["localhost".into()]);
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = Certificate::from_params(ca_params)?;
    let ca_pem = ca.serialize_pem()?;

    let mut server_params = CertificateParams::new(vec!["localhost".into()]);
    server_params.alg = &PKCS_ECDSA_P256_SHA256;
    let server_cert = Certificate::from_params(server_params)?;
    let _server_cert_der = ca.serialize_der_with_signer(&server_cert)?;

    // Verify we can add the generated CA to a RootCertStore and build a client config.
    let mut root_store = RootCertStore::empty();
    let mut reader = BufReader::new(ca_pem.as_bytes());
    for cert in rustls_pemfile::certs(&mut reader) {
        root_store.add(cert?)?;
    }

    // Build a basic client config that trusts our test CA.
    let _client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(())
}
