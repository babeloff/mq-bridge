use anyhow::Result;
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::RootCertStore;

#[cfg(feature = "rustls")]
#[tokio::test]
async fn tls_handshake_example() -> Result<()> {
    // Install a rustls CryptoProvider for this test (feature-gated).
    #[cfg(feature = "rustls-aws-lc")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    #[cfg(all(feature = "rustls-ring", not(feature = "rustls-aws-lc")))]
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Generate a test CA and a server certificate signed by it.
    let mut ca_params = CertificateParams::new(vec!["localhost".into()])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate()?;
    let ca = ca_params.self_signed(&ca_key)?;
    let ca_issuer = Issuer::new(ca_params, ca_key);

    let server_params = CertificateParams::new(vec!["localhost".into()])?;
    let server_key = KeyPair::generate()?;
    let _server_cert = server_params.signed_by(&server_key, &ca_issuer)?;

    // Verify we can add the generated CA to a RootCertStore and build a client config.
    let mut root_store = RootCertStore::empty();
    root_store.add(ca.der().clone())?;

    // Build a basic client config that trusts our test CA.
    let _client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(())
}
