use mysql::consts::ColumnType;
use serde::Serialize;
use serde::Serializer;

#[derive(serde::Serialize)]
pub struct Row<'a>(Vec<Value<'a>>);

pub struct Value<'a>(&'a mysql::Value, ColumnType);

impl<'a> Row<'a> {
    pub fn new(column_types: &[ColumnType], values: &'a [mysql::Value]) -> Self {
        let mut row = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            row.push(Value(&values[i], column_types[i]))
        }
        Self(row)
    }
}

impl<'a> Serialize for Value<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use mysql::Value::*;
        match (self.0, self.1) {
            (NULL, _) => serializer.serialize_unit(),
            (Int(x), _) => serializer.serialize_i64(*x),
            (UInt(x), _) => serializer.serialize_u64(*x),
            (Float(x), _) => serializer.serialize_f32(*x),
            (Double(x), _) => serializer.serialize_f64(*x),
            (Date(year, month, day, hour, minute, second, microsecond), _) => serializer
                .serialize_str(
                    format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{}",
                        year, month, day, hour, minute, second, microsecond
                    )
                    .as_str(),
                ),
            (Time(is_negative, days, hours, minutes, seconds, microseconds), _) => serializer
                .serialize_str(
                    format!(
                        "{}{:02}:{:02}:{:02}.{}",
                        if *is_negative { "-" } else { "" },
                        days * 24 + *hours as u32,
                        minutes,
                        seconds,
                        microseconds
                    )
                    .as_str(),
                ),
            (Bytes(b), t) => serialize_mysql_bytes(serializer, t, b),
        }
    }
}

fn serialize_mysql_bytes<S>(
    serializer: S,
    column_type: ColumnType,
    bytes: &[u8],
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use mysql::consts::ColumnType::*;

    match column_type {
        MYSQL_TYPE_DATE
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_GEOMETRY
        | MYSQL_TYPE_JSON
        | MYSQL_TYPE_NEWDATE
        | MYSQL_TYPE_SET
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_TIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2
        | MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING => serializer.serialize_str(
            std::str::from_utf8(bytes).expect(format!("valid utf8 for {column_type:?}").as_str()),
        ),
        MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_BLOB => {
            serializer.serialize_bytes(bytes)
        }
        MYSQL_TYPE_INT24 => serializer.serialize_i32(
            std::str::from_utf8(bytes)
                .expect("valid utf8")
                .parse()
                .expect("valid number"),
        ),
        MYSQL_TYPE_NULL => serializer.serialize_unit(),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_DOUBLE | MYSQL_TYPE_FLOAT | MYSQL_TYPE_NEWDECIMAL => {
            serializer.serialize_f64(
                std::str::from_utf8(bytes)
                    .expect("valid utf8")
                    .parse()
                    .expect("valid decimal"),
            )
        }
        MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG => serializer.serialize_i64(
            std::str::from_utf8(bytes)
                .expect("valid utf8")
                .parse()
                .expect("valid long"),
        ),
        MYSQL_TYPE_YEAR | MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT => serializer.serialize_i32(
            std::str::from_utf8(bytes)
                .expect("valid utf8")
                .parse()
                .expect("valid short"),
        ),
        /*
        MYSQL_TYPE_BIT
        MYSQL_TYPE_TYPED_ARRAY
        MYSQL_TYPE_UNKNOWN
                    */
        _ => serializer.serialize_bytes(bytes),
    }
}
