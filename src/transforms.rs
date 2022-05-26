use std::net::Ipv6Addr;
use std::ops::Range;
use std::ops::Sub;
use std::str::from_utf8;
use std::str::FromStr;

use color_eyre::Result;
use fake::faker::address::en::*;
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::{IPv4, IPv6, MACAddress, SafeEmail, Username};
use fake::faker::lorem::en::Words;
use fake::faker::name::en::*;
use fake::faker::phone_number::en::PhoneNumber;
use fake::Fake;
use itertools::Itertools;
use mysql::Value;
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;
use serde::Deserialize;
use xxhash_rust::xxh3;

static ALPHANUM_LOWER: &str = "abcdefghijklmnopqrstuvwxyz0123456789";

#[derive(Deserialize, Debug)]
pub struct Transform {
    pub column: String,
    pub kind: TransformKind,
    pub config: Option<String>,
    pub config2: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TransformKind {
    Addr1,
    Addr2,
    City,
    CountryCode,
    DomainHash,
    Email,
    EmailHash,
    Empty,
    Firstname,
    Fullname,
    Hostname,
    Ipv4,
    Ipv6,
    Ipv6Bin,
    Lastname,
    LoremIpsum,
    MacAddress,
    Null,
    Organization,
    Phone,
    PostalCode,
    Regex,
    RandomAlphanum,
    RandomInt,
    RandomMoney,
    Replace,
    ReplaceIfNotEmpty,
    StateCode,
    Username,
}

impl TransformKind {
    pub fn apply(
        &self,
        rng: &mut impl Rng,
        config: Option<&String>,
        config2: Option<&String>,
        value: &mut Value,
    ) {
        match self {
            TransformKind::Empty => *value = Value::Bytes(Vec::new()),
            TransformKind::Replace => match config {
                Some(s) => *value = Value::Bytes(s.as_bytes().to_owned()),
                None => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::ReplaceIfNotEmpty => match (&value, config) {
                (Value::Bytes(b), Some(s)) if !b.is_empty() => {
                    *value = Value::Bytes(s.as_bytes().to_owned())
                }
                _ => *value = Value::Bytes(Vec::new()),
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
                    _ => hash_email("".as_bytes()),
                };
                *value = Value::Bytes(email.into())
            }
            TransformKind::Email => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(SafeEmail().fake_with_rng::<String, _>(rng).into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
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
            TransformKind::RandomAlphanum => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let len = config.and_then(|v| v.parse().ok()).unwrap_or(6);
                    *value = Value::Bytes(random_alphanum(rng, len).into())
                }
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::LoremIpsum => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let len = config.and_then(|v| v.parse().ok()).unwrap_or(20);
                    *value = Value::Bytes(
                        Words(0..len) // use len here to generate many more words than we'll need
                            .fake_with_rng::<Vec<String>, _>(rng)
                            .join(" ")
                            .chars()
                            .take(len)
                            .collect::<String>()
                            .into(),
                    )
                }
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Ipv6Bin => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let ip: Ipv6Addr = IPv6().fake_with_rng(rng);
                    *value = Value::Bytes(ip.octets().to_vec())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Phone => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(PhoneNumber().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::StateCode => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(StateAbbr().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::CountryCode => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(CountryCode().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::MacAddress => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    *value = Value::Bytes(MACAddress().fake_with_rng::<String, _>(rng).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::RandomInt => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let r = match config {
                        Some(s) => parse_range(s).unwrap_or(0..i32::MAX),
                        None => 0..i32::MAX,
                    };
                    *value = Value::Bytes(format!("{}", rng.gen_range::<i32, _>(r)).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::RandomMoney => match value {
                Value::Bytes(b) if !b.is_empty() => {
                    let max: f32 = match config {
                        Some(s) => s.parse().unwrap_or(500.00),
                        None => 500.00,
                    };
                    let n = rng.gen_range::<f32, _>(0f32..max);
                    *value = Value::Bytes(format!("{:.02}", n).into())
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Regex => regex_replace(value, config, config2),
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
    format!("{}@example", hash_to_charset(b, 11, ALPHANUM_LOWER))
}

fn hash_domain(b: &[u8]) -> String {
    format!("{}.example", hash_to_charset(b, 6, ALPHANUM_LOWER))
}

fn hash_to_charset(b: &[u8], len: usize, charset: &str) -> String {
    let charset_len = charset.len() as u8;

    xxh3::xxh3_128(b)
        .to_le_bytes()
        .iter()
        .take(len)
        .map(|b| *b % charset_len)
        .map(|n| {
            charset
                .chars()
                .nth(n.into())
                .expect("invalid offset of charset")
        })
        .collect()
}

fn parse_range<T>(s: &str) -> Result<Range<T>>
where
    T: PartialOrd<T> + FromStr + Sub + Sub<Output = T> + Copy,
    <T as FromStr>::Err: Sync + Send + std::error::Error + 'static,
{
    if let Some((a, b)) = s.splitn(2, '-').collect_tuple() {
        let start = a.parse()?;
        let end = b.parse()?;
        Ok(start..end)
    } else {
        let end = s.parse()?;
        Ok((end - end)..end)
    }
}

fn regex_replace(
    value: &mut mysql::Value,
    maybe_pattern: Option<&String>,
    maybe_replace: Option<&String>,
) {
    let s = match value {
        Value::Bytes(b) => String::from_utf8(b.to_vec()).unwrap_or("".to_owned()),
        _ => String::new(),
    };

    let pattern = match maybe_pattern {
        Some(s) if !s.is_empty() => s,
        _ => panic!("regex requires config: for pattern and config2: for replacement"),
    };

    let replace = match maybe_replace {
        Some(s) => s.as_str(),
        _ => "",
    };

    let re = Regex::new(pattern).expect("invalid regex");
    let new = re.replace_all(&s, replace);
    *value = Value::Bytes(new.to_string().into());
}
