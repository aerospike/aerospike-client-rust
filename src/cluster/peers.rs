use crate::errors::{ErrorKind, Result};
use crate::net::{Host, ToHosts};
use logos::{Lexer, Logos};

#[derive(Logos, Debug, PartialEq)]
enum Token {
    #[token("[")]
    OpenBracket,

    #[token("]")]
    CloseBracket,

    #[regex("[0-9a-zA-Z-./_: ]+")]
    Text,

    #[error]
    #[regex(r"[,]+", logos::skip)]
    Error,
}

fn parse_error(lex: &Lexer<Token>, source: &str) -> String {
    format!(
        "Failed to parse peers: {}, at {:?} ({})",
        source,
        lex.span(),
        lex.slice()
    )
}

pub fn parse_peers_info(info_peers: &str) -> Result<Vec<Host>> {
    let mut lex = Token::lexer(info_peers);

    let _peer_gen = match lex.next() {
        Some(Token::Text) => lex.slice(),
        _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
    };
    let default_port_str = match lex.next() {
        Some(Token::Text) => lex.slice(),
        _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
    };

    let default_port = match default_port_str.parse::<u16>() {
        Ok(port) => port,
        Err(_) => bail!(ErrorKind::BadResponse(format!(
            "Invalid default port: {}",
            default_port_str
        ))),
    };

    match lex.next() {
        Some(Token::OpenBracket) => parse_peers(info_peers, &mut lex, default_port),
        _ => Ok(Vec::new()),
    }
}

fn parse_peers(info_peers: &str, lex: &mut Lexer<Token>, default_port: u16) -> Result<Vec<Host>> {
    let mut peers = Vec::new();
    loop {
        match lex.next() {
            Some(Token::OpenBracket) => peers.extend(parse_peer(info_peers, lex, default_port)?),
            Some(Token::CloseBracket) => return Ok(peers),
            _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
        }
        lex.next(); // Close brackets
    }
}

fn parse_peer(info_peers: &str, lex: &mut Lexer<Token>, default_port: u16) -> Result<Vec<Host>> {
    let _id = match lex.next() {
        Some(Token::Text) => lex.slice(),
        _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
    };

    let mut token = lex.next();
    if Some(Token::Text) == token {
        let _tls_hostname = lex.slice();
        token = lex.next();
    }

    match token {
        Some(Token::OpenBracket) => (),
        _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
    };

    let hosts = match lex.next() {
        Some(Token::Text) => lex.slice(),
        _ => bail!(ErrorKind::BadResponse(parse_error(&lex, info_peers))),
    }
    .to_hosts_with_default_port(default_port)?;

    lex.next(); // Close brackets
    Ok(hosts)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn parse_peers_works() {
        let work = "6,3000,[[12A0,aerospike.com,[1.2.3.4:4333]],[BB9040011AC4202,,[10.11.12.13]],[11A1,,[localhost]]]";
        let fail = "6,3foobar,[[12A0,aerospike.com,[1.2.3.4:4333]],[11A1,,[10.11.12.13:4333]]]";
        let empty = "6,3000,[]";
        assert!(parse_peers_info(fail).is_err());
        let work = parse_peers_info(work).unwrap();
        println!("{:?}", work);
        assert!(
            work == vec![
                Host {
                    name: "1.2.3.4".to_string(),
                    port: 4333
                },
                Host {
                    name: "10.11.12.13".to_string(),
                    port: 3000
                },
                Host {
                    name: "localhost".to_string(),
                    port: 3000
                }
            ]
        );
        let empty = parse_peers_info(empty).unwrap();
        assert!(empty == vec![]);
    }
}
