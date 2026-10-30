// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! A parser for the EC2 and STS XML responses: elements and text only, no
//! DTD (a document type declaration is rejected) and no external entities.

#![allow(clippy::redundant_pub_crate)]

use super::http::Response;
use crate::errors::Error;

/// An element with its text content (own text, not the descendants') and
/// child elements in document order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Element {
    pub name: String,
    pub text: String,
    pub children: Vec<Element>,
}

impl Element {
    /// The direct child elements named `name`.
    pub(crate) fn children<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
        self.children.iter().filter(move |c| c.name == name)
    }

    /// The first direct child element named `name`.
    pub(crate) fn child(&self, name: &str) -> Option<&Element> {
        self.children.iter().find(|c| c.name == name)
    }

    /// The trimmed text of the first direct child named `name`.
    pub(crate) fn text(&self, name: &str) -> Option<String> {
        self.child(name).map(|e| e.text.trim().to_string())
    }

    /// The trimmed text of the first descendant named `name`, in document
    /// order.
    pub(crate) fn find(&self, name: &str) -> Option<String> {
        self.find_element(name).map(|e| e.text.trim().to_string())
    }

    pub(crate) fn find_element(&self, name: &str) -> Option<&Element> {
        for child in &self.children {
            if child.name == name {
                return Some(child);
            }
            if let Some(found) = child.find_element(name) {
                return Some(found);
            }
        }
        None
    }
}

/// Parses a document and returns its root element.
pub(crate) fn parse(body: &[u8]) -> Result<Element, String> {
    let text = std::str::from_utf8(body).map_err(|e| format!("invalid UTF-8: {e}"))?;
    let mut parser = Parser { s: text, pos: 0 };
    let mut root = None;
    while let Some(token) = parser.token()? {
        match token {
            Token::Open(name, self_closing) => {
                if root.is_some() {
                    return Err("more than one root element".into());
                }
                let element = if self_closing {
                    Element {
                        name,
                        ..Element::default()
                    }
                } else {
                    parser.element(name)?
                };
                root = Some(element);
            }
            Token::Close(name) => return Err(format!("unexpected closing tag </{name}>")),
            Token::Text(t) if t.trim().is_empty() => {}
            Token::Text(_) => return Err("text outside the root element".into()),
        }
    }
    root.ok_or_else(|| "empty document".to_string())
}

enum Token {
    Open(String, bool),
    Close(String),
    Text(String),
}

struct Parser<'a> {
    s: &'a str,
    pos: usize,
}

impl Parser<'_> {
    /// Reads the children of an element whose opening tag has been consumed,
    /// up to and including its closing tag.
    fn element(&mut self, name: String) -> Result<Element, String> {
        let mut element = Element {
            name,
            ..Element::default()
        };
        loop {
            match self.token()? {
                None => return Err(format!("unterminated element <{}>", element.name)),
                Some(Token::Text(t)) => element.text.push_str(&t),
                Some(Token::Open(child, self_closing)) => {
                    let child = if self_closing {
                        Element {
                            name: child,
                            ..Element::default()
                        }
                    } else {
                        self.element(child)?
                    };
                    element.children.push(child);
                }
                Some(Token::Close(close)) => {
                    if close != element.name {
                        return Err(format!(
                            "mismatched closing tag </{close}> for <{}>",
                            element.name
                        ));
                    }
                    return Ok(element);
                }
            }
        }
    }

    fn rest(&self) -> &str {
        &self.s[self.pos..]
    }

    fn token(&mut self) -> Result<Option<Token>, String> {
        loop {
            let rest = self.rest();
            if rest.is_empty() {
                return Ok(None);
            }
            if let Some(after) = rest.strip_prefix("<?") {
                let end = after.find("?>").ok_or("unterminated processing instruction")?;
                self.pos += 2 + end + 2;
                continue;
            }
            if let Some(after) = rest.strip_prefix("<!--") {
                let end = after.find("-->").ok_or("unterminated comment")?;
                self.pos += 4 + end + 3;
                continue;
            }
            if let Some(after) = rest.strip_prefix("<![CDATA[") {
                let end = after.find("]]>").ok_or("unterminated CDATA section")?;
                let text = after[..end].to_string();
                self.pos += 9 + end + 3;
                return Ok(Some(Token::Text(text)));
            }
            if rest.starts_with("<!") {
                return Err("document type declarations are not allowed".into());
            }
            if let Some(after) = rest.strip_prefix("</") {
                let end = after.find('>').ok_or("unterminated closing tag")?;
                let name = after[..end].trim().to_string();
                self.pos += 2 + end + 1;
                return Ok(Some(Token::Close(name)));
            }
            if let Some(after) = rest.strip_prefix('<') {
                let end = after.find('>').ok_or("unterminated tag")?;
                let inside = &after[..end];
                let self_closing = inside.ends_with('/');
                let inside = inside.trim_end_matches('/');
                let name = inside
                    .split(|c: char| c.is_whitespace())
                    .next()
                    .unwrap_or_default()
                    .to_string();
                if name.is_empty() {
                    return Err("empty tag name".into());
                }
                self.pos += 1 + end + 1;
                return Ok(Some(Token::Open(name, self_closing)));
            }
            let end = rest.find('<').unwrap_or(rest.len());
            let text = decode_entities(&rest[..end])?;
            self.pos += end;
            return Ok(Some(Token::Text(text)));
        }
    }
}

fn decode_entities(text: &str) -> Result<String, String> {
    if !text.contains('&') {
        return Ok(text.to_string());
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let end = after.find(';').ok_or("unterminated entity")?;
        let entity = &after[..end];
        let decoded = match entity {
            "lt" => '<',
            "gt" => '>',
            "amp" => '&',
            "quot" => '"',
            "apos" => '\'',
            _ => {
                let code = entity
                    .strip_prefix('#')
                    .ok_or_else(|| format!("unknown entity &{entity};"))?;
                let value = match code.strip_prefix('x') {
                    Some(hex) => u32::from_str_radix(hex, 16),
                    None => code.parse::<u32>(),
                }
                .map_err(|_| format!("invalid character reference &{entity};"))?;
                char::from_u32(value).ok_or_else(|| format!("invalid character reference &{entity};"))?
            }
        };
        out.push(decoded);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// An error for an AWS Query API error response, with the HTTP status, the
/// AWS error code and message and the request id when present. Each secret
/// is removed from the message.
pub(crate) fn error(action: &str, response: &Response, secrets: &[&str]) -> Error {
    let mut code = None;
    let mut message = None;
    let mut request_id = None;
    if let Ok(root) = parse(&response.body) {
        code = root.find("Code");
        message = root.find("Message");
        request_id = root.find("RequestID").or_else(|| root.find("RequestId"));
    }
    if request_id.is_none() {
        request_id = response.headers.get("x-amzn-requestid").cloned();
    }
    let mut text = format!(
        "AWS EC2 seed provider: {action} failed: HTTP {}",
        response.status
    );
    if let Some(code) = code {
        text.push(' ');
        text.push_str(&code);
    }
    if let Some(message) = message {
        text.push_str(": ");
        text.push_str(&message);
    }
    if let Some(request_id) = request_id {
        use std::fmt::Write as _;
        let _ = write!(text, " (request id {request_id})");
    }
    Error::client_error(redact(&text, secrets))
}

/// Replaces each non-empty secret in `text`.
pub(crate) fn redact(text: &str, secrets: &[&str]) -> String {
    let mut text = text.to_string();
    for secret in secrets {
        if !secret.is_empty() {
            text = text.replace(secret, "<redacted>");
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_elements_text_and_entities() {
        let doc = br#"<?xml version="1.0" encoding="UTF-8"?>
<!-- a comment -->
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <requestId>abc</requestId>
  <reservationSet>
    <item><instancesSet><item><privateIpAddress>10.0.0.1</privateIpAddress><ipAddress/></item></instancesSet></item>
    <item><instancesSet><item><privateIpAddress> 10.0.0.2 </privateIpAddress><tag>a &amp; b &lt;c&gt; &#x41;&#66;</tag></item></instancesSet></item>
  </reservationSet>
  <nextToken><![CDATA[tok<1>]]></nextToken>
</DescribeInstancesResponse>"#;
        let root = parse(doc).unwrap();
        assert_eq!(root.name, "DescribeInstancesResponse");
        assert_eq!(root.text("requestId").as_deref(), Some("abc"));
        let reservations: Vec<&Element> = root.child("reservationSet").unwrap().children("item").collect();
        assert_eq!(reservations.len(), 2);
        let first = reservations[0].child("instancesSet").unwrap().child("item").unwrap();
        assert_eq!(first.text("privateIpAddress").as_deref(), Some("10.0.0.1"));
        assert_eq!(first.text("ipAddress").as_deref(), Some(""));
        assert_eq!(first.text("dnsName"), None);
        let second = reservations[1].child("instancesSet").unwrap().child("item").unwrap();
        assert_eq!(second.text("privateIpAddress").as_deref(), Some("10.0.0.2"));
        assert_eq!(second.text("tag").as_deref(), Some("a & b <c> AB"));
        assert_eq!(root.text("nextToken").as_deref(), Some("tok<1>"));
        assert_eq!(root.find("privateIpAddress").as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn rejects_doctype_and_malformed_documents() {
        let err = parse(b"<!DOCTYPE foo [<!ENTITY x SYSTEM \"file:///etc/passwd\">]><a>&x;</a>").unwrap_err();
        assert!(err.contains("document type declarations"), "{err}");
        assert!(parse(b"<a><b></a>").is_err());
        assert!(parse(b"<a>").is_err());
        assert!(parse(b"").is_err());
        assert!(parse(b"<a>&bogus;</a>").is_err());
    }

    #[test]
    fn error_carries_code_message_and_request_id_without_secrets() {
        let body = b"<Response><Errors><Error><Code>UnauthorizedOperation</Code><Message>You are not authorized. SECRET</Message></Error></Errors><RequestID>ea966190</RequestID></Response>";
        let response = Response {
            status: 403,
            headers: std::collections::HashMap::default(),
            body: body.to_vec(),
        };
        let err = error("DescribeInstances", &response, &["SECRET"]);
        assert_eq!(
            err.base_message(),
            "AWS EC2 seed provider: DescribeInstances failed: HTTP 403 UnauthorizedOperation: \
             You are not authorized. <redacted> (request id ea966190)"
        );
        // Falls back to the header for the request id, and copes with a non-XML body.
        let mut headers = std::collections::HashMap::new();
        headers.insert("x-amzn-requestid".to_string(), "hdr-id".to_string());
        let response = Response {
            status: 500,
            headers,
            body: b"not xml".to_vec(),
        };
        assert_eq!(
            error("STS AssumeRoleWithWebIdentity", &response, &[]).base_message(),
            "AWS EC2 seed provider: STS AssumeRoleWithWebIdentity failed: HTTP 500 (request id hdr-id)"
        );
    }
}
