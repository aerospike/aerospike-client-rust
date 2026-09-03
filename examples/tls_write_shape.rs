//! Counts socket-level writes per request on the TLS path.
//!
//! `Connection::flush` does `write_all(buf).await?; flush().await` on a
//! `tokio_rustls` stream. Nagle acts on kernel segments, so what matters is
//! how many separate `write` syscalls rustls issues underneath that one
//! logical request. One write is safe (measured: no stall). Two or more is
//! the split-write shape that stalls for TCP_DELACK_MIN = 40ms.
//!
//! This wraps the TcpStream in a counting adapter and reports the write
//! sizes rustls actually emits, for payloads either side of the 16 KiB TLS
//! record limit.
//!
//! Run:  cargo run --example tls_write_shape --features rt-tokio,tls

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{RootCertStore, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const TEST_CERT_PEM: &[u8] = b"\
-----BEGIN CERTIFICATE-----
MIIBkjCCATigAwIBAgIUBd8CSag9UwVN+CmSCt1fHb95AXowCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDgyMTAxMDcyOFoYDzIxMjYwNzI4
MDEwNzI4WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARaDG4MJdt4ujwjndx1baO6lEZF2JIggiXBCqFUdjj6IPPzkDZtMO1f
U3lfoCm6z5EGqRhWg8An6dxdhFCdc2AZo2YwZDAdBgNVHQ4EFgQUkmu2kSnGV8kL
9Fd/i3OqX64gxlowHwYDVR0jBBgwFoAUkmu2kSnGV8kL9Fd/i3OqX64gxlowFAYD
VR0RBA0wC4IJbG9jYWxob3N0MAwGA1UdEwEB/wQCMAAwCgYIKoZIzj0EAwIDSAAw
RQIgD7tXhh5Ldb6hp8VcfcmI8MZQf5TrJSJO2SEXAVOzcmECIQCfDq3M7/QhO7f/
+5kpUI6o5q5Lp53WCGOvmKjvMmEfKg==
-----END CERTIFICATE-----
";

const TEST_KEY_PEM: &[u8] = b"\
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZcTGiz3ft6sc5Q+L
rHUGuKRhKz67vcrzXrqQFgLuGO+hRANCAARaDG4MJdt4ujwjndx1baO6lEZF2JIg
giXBCqFUdjj6IPPzkDZtMO1fU3lfoCm6z5EGqRhWg8An6dxdhFCdc2AZ
-----END PRIVATE KEY-----
";

fn self_signed() -> (ServerConfig, RootCertStore) {
    let cert = CertificateDer::from_pem_slice(TEST_CERT_PEM).expect("test certificate");
    let key = PrivateKeyDer::from_pem_slice(TEST_KEY_PEM).expect("test key");
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    (server, roots)
}

/// Records the length of every `poll_write` that reaches the socket, plus an
/// interleaved trace of reads. Whether two small writes are *consecutive*
/// (no read between them) is what decides if Nagle can deadlock: a read
/// implies the peer's reply, which carries the ACK that releases the sender.
struct Counting {
    inner: TcpStream,
    writes: Arc<Mutex<Vec<usize>>>,
    trace: Arc<Mutex<Vec<String>>>,
}

impl tokio::io::AsyncWrite for Counting {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = res {
            self.writes.lock().unwrap().push(n);
            self.trace.lock().unwrap().push(format!("W{n}"));
        }
        res
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl tokio::io::AsyncRead for Counting {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = res {
            let n = buf.filled().len() - before;
            if n > 0 {
                self.trace.lock().unwrap().push(format!("R{n}"));
            }
        }
        res
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let (server_config, roots) = self_signed();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let sizes: Vec<usize> = vec![512, 4096, 8192, 16_000, 16_384, 16_500, 32_768, 65_536, 262_144];
    let server_sizes = sizes.clone();

    tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(sock).await.unwrap();
        for size in server_sizes {
            let mut buf = vec![0u8; size];
            // Read the request in full before replying: the server cannot
            // piggyback an ACK on a response it cannot yet produce.
            tls.read_exact(&mut buf).await.unwrap();
            tls.write_all(&[0u8; 8]).await.unwrap();
            tls.flush().await.unwrap();
        }
    });

    let writes = Arc::new(Mutex::new(Vec::new()));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let sock = TcpStream::connect(addr).await.unwrap();
    sock.set_nodelay(true).unwrap();
    let counting = Counting {
        inner: sock,
        writes: writes.clone(),
        trace: trace.clone(),
    };

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut tls = TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), counting)
        .await
        .unwrap();

    let handshake = writes.lock().unwrap().clone();
    println!(
        "TLS handshake: {} socket writes {:?}",
        handshake.len(),
        handshake
    );
    println!(
        "handshake syscall trace (W=write, R=read): {}",
        trace.lock().unwrap().join(" ")
    );
    println!("\nOne `write_all(buf) + flush()` per row -- the Connection::flush shape.\n");
    println!(
        "{:>9} | {:>7} | {}",
        "payload", "writes", "socket write sizes"
    );
    println!("{}", "-".repeat(76));

    let mut reply = [0u8; 8];
    for size in sizes {
        writes.lock().unwrap().clear();
        let buf = vec![0xABu8; size];

        tls.write_all(&buf).await.unwrap();
        tls.flush().await.unwrap();
        tls.read_exact(&mut reply).await.unwrap();

        let w = writes.lock().unwrap().clone();
        let verdict = if w.len() > 1 { "  <== SPLIT" } else { "" };
        println!("{:>9} | {:>7} | {:?}{}", size, w.len(), w, verdict);
    }
}
