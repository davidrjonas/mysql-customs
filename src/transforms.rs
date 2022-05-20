use std::str::from_utf8;

use fake::faker::address::en::*;
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::{IPv4, IPv6, Username};
use fake::faker::name::en::*;
use fake::Fake;
use mysql::Value;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use xxhash_rust::xxh3;

#[derive(Deserialize, Debug)]
pub struct Transform {
    pub column: String,
    pub kind: TransformKind,
    pub config: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TransformKind {
    Addr1,
    Addr2,
    City,
    DomainHash,
    EmailHash,
    Empty,
    Firstname,
    Fullname,
    Hostname,
    Ipv4,
    Ipv6,
    Lastname,
    Null,
    Organization,
    PostalCode,
    Replace,
    Username,
}

impl TransformKind {
    pub fn apply(&self, rng: &mut impl Rng, config: Option<&String>, value: &mut Value) {
        match self {
            TransformKind::Empty => *value = Value::Bytes(Vec::new()),
            TransformKind::Replace => match config {
                Some(s) => *value = Value::Bytes(s.as_bytes().to_owned()),
                None => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Fullname => {
                let name: String = Name().fake_with_rng(rng);
                *value = Value::Bytes(name.into())
            }
            TransformKind::Firstname => {
                let name: String = FirstName().fake_with_rng(rng);
                *value = Value::Bytes(name.into())
            }
            TransformKind::Lastname => {
                let name: String = LastName().fake_with_rng(rng);
                *value = Value::Bytes(name.into())
            }
            TransformKind::EmailHash => {
                let email = match value {
                    Value::Bytes(b) => hash_email(b),
                    _ => random_alphanum(rng, 10),
                };
                *value = Value::Bytes(email.into())
            }
            TransformKind::Organization => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let name: String = CompanyName().fake_with_rng(rng);
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Addr1 => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let name = format!(
                        "{} {} {}",
                        rng.gen::<u8>(),
                        StreetName().fake_with_rng::<String, _>(rng),
                        StreetSuffix().fake_with_rng::<&str, _>(rng)
                    );
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Addr2 => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let name: String = SecondaryAddress().fake_with_rng(rng);
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::City => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let name: String = CityName().fake_with_rng(rng);
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::PostalCode => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let name: String = PostCode().fake_with_rng(rng);
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Hostname => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let orig = from_utf8(b).unwrap_or("");
                    let name = match orig.len() {
                        0 | 1 | 2 => orig.to_owned(),
                        len => format!("{}{}", &orig[0..2], random_alphanum_lower(rng, len - 2)),
                    };
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::DomainHash => match value {
                Value::Bytes(b) if !b.is_empty() => *value = Value::Bytes(hash_domain(b).into()),
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Ipv4 => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(IPv4().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Ipv6 => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(IPv6().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Username => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(Username().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Null => *value = Value::NULL,
        }
    }
}

fn random_alphanum(rng: &mut impl Rng, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn random_alphanum_lower(rng: &mut impl Rng, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
        .to_ascii_lowercase()
}

fn hash_email(b: &[u8]) -> String {
    let mut name = base64::encode(xxh3::xxh3_64(b).to_le_bytes());
    name.truncate(11);
    format!("{name}@example.com",)
}

fn hash_domain(b: &[u8]) -> String {
    let charset = "abcdefghijklmnopqrstuvwxyz0123456789";
    let encoded: String = xxh3::xxh3_64(b)
        .to_le_bytes()
        .iter()
        .take(6)
        .map(|b| (*b as usize) % charset.len())
        .map(|n| charset.chars().nth(n).expect("invalid offset of charset"))
        .collect();
    format!("{}.example", encoded)
}
