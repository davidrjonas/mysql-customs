mysql-customs
=============

A MySQL Sanitizer for Development.

This program reads MySQL tables then writes the data as CSV files transforming the data in the process. 

Usage
-----

```
mysql-customs -d mysql://user:pass@localhost:3306/db -c config.yaml --compress -t output_dir/
```

Configuration
-------------

Which databases, tables, and transforms are controlled with a yaml configuration file. Here is the general schema:

```yaml
databases:
  <database name>:
    tables:
      <table name>:
        filter: <where clause>
        order_column: <order by when not `id`>
        transforms:
          - column: <column name>
            kind: <kind of transform, see below>
            config: <config for the transform if required>
```


### Transforms

- `addr1`: fake data if not empty
- `addr2`: fake data if not empty
- `city`: fake data if not empty
- `email_hash`: [hash of email]@example.com
- `empty`: replace with empty string
- `firstname`: fake data
- `lastname`: fake data
- `organization`: fake company if not empty
- `postal_code`: fake data
- `replace`: replace with value in `config:` field
