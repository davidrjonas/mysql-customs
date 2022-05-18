use fake::faker::address::en::*;
use fake::faker::company::en::CompanyName;
use fake::faker::name::en::*;
use fake::Fake;
use mysql::Value;
use serde::Deserialize;
use xxhash_rust::xxh3;

#[derive(Deserialize)]
pub struct Transform {
    pub column: String,
    pub kind: TransformKind,
    pub config: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformKind {
    Empty,
    Replace,
    Firstname,
    Fullname,
    Lastname,
    EmailHash,
    Organization,
    Addr1,
    Addr2,
    City,
    PostalCode,
}

impl TransformKind {
    pub fn apply(&self, config: Option<&String>, value: &mut Value) {
        match self {
            TransformKind::Empty => *value = Value::Bytes(Vec::new()),
            TransformKind::Replace => match config {
                Some(s) => *value = Value::Bytes(s.as_bytes().to_owned()),
                None => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Fullname => {
                let name: String = Name().fake();
                *value = Value::Bytes(name.into())
            }
            TransformKind::Firstname => {
                let name: String = FirstName().fake();
                *value = Value::Bytes(name.into())
            }
            TransformKind::Lastname => {
                let name: String = LastName().fake();
                *value = Value::Bytes(name.into())
            }
            TransformKind::EmailHash => {
                let email = match value {
                    Value::Bytes(b) => hash_email(b),
                    _ => random_string(10),
                };
                *value = Value::Bytes(email.into())
            }
            TransformKind::Organization => match value {
                Value::Bytes(b) if b.len() > 0 => {
                    let name: String = CompanyName().fake();
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Addr1 => match value {
                Value::Bytes(b) if b.len() > 0 => {
                    let name = format!(
                        "{} {} {}",
                        rand::random::<u8>(),
                        StreetName().fake::<String>(),
                        StreetSuffix().fake::<&str>()
                    );
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::Addr2 => match value {
                Value::Bytes(b) if b.len() > 0 => {
                    let name: String = SecondaryAddress().fake();
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::City => match value {
                Value::Bytes(b) if b.len() > 0 => {
                    let name: String = CityName().fake();
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
            TransformKind::PostalCode => match value {
                Value::Bytes(b) if b.len() > 0 => {
                    let name: String = PostCode().fake();
                    *value = Value::Bytes(name.into());
                }
                Value::Bytes(_) => {}
                _ => *value = Value::Bytes(Vec::new()),
            },
        }
    }
}

fn random_string(len: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn hash_email(b: &[u8]) -> String {
    let mut name = base64::encode(xxh3::xxh3_64(b).to_le_bytes());
    name.truncate(11);
    format!("{name}@example.com",)
}
