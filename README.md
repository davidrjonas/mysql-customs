mysql-customs
=============

A MySQL subsetter and sanitizer for development environments.

This program reads MySQL tables then writes the data as CSV files filtering and transforming the data in the process.

> You end up with customized stuff, having gone through customs, as is customary.
>      - Chuck Musser

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
    trace_filters: # optional
      - name: <filter name>
        match_column: [<array of column names to join against source.column, if the exist>]
        source:
          db: <db name, may be different than current>
          table: <table name>
          column: <the join column, such as "id">
          filter: <where clause>
    tables:
      <table name>:
        filter: <where clause> # optional
        order_column: <order by when not `id`> # optional
        transforms: # optional
          - column: <column name>
            kind: <kind of transform, see below>
            config: <config for the transform if required>
        related_only: # optional
          table: <name of the related table>
          column: <name of column on related table to join to>
          foreign_column: <name of column on the current table that joins to the related_only.column>
```


### Transforms

#### General Manipulation

- `empty`: replace with empty string
- `null`: replace with \N
- `replace`: replace with value in `config:` field
- `replace_if_not_empty`: replace with value in `config:` field if value is not "" or null
- `regex`: match value with `config:` and replace all with `config2:`. See https://docs.rs/regex

#### Hashed Data

Helpful for maintaining uniqueness.

- `email_hash`: [hash of email]@example.com. See also `email` for fake emails when uniqueness doesn't matter.
- `domain_hash`: 4 char hash of original domain plus example.tld where tld is chosen by hash of original

#### Fake Data

- `addr1`: fake data if not empty
- `addr2`: fake data if not empty
- `city`: fake data if not empty
- `country_code`: fake data if not empty. Two-letter code.
- `email`: fake data if not empty. See also `email_hash` for unique emails.
- `firstname`: fake data
- `fullname`: fake data
- `ipv4`: fake data
- `ipv6_bin`: fake data, in binary format
- `ipv6`: fake data
- `lastname`: fake data
- `mac_address`: fake data if not empty
- `organization`: fake company if not empty
- `phone`: fake data if not empty
- `postal_code`: fake data
- `state_code`: fake data if not empty. Two-letter code.
- `username`: fake data if not empty

#### Random Data

- `hostname`: keep the first two chars, replace the rest with random alphnums.
- `lorem_ipsum`: string of `config:` chars long (or length of original) from lorem ipsum.
- `random_alphanum`: random string of lowercase alphanum of length `config:` if not empty
- `random_int`: i32 in the range of `config:` with format "start-end", if not empty
- `random_money`: f32 up to `config:`, default 500.00, if not empty

Concepts
--------

### Trace Filters

Trace filters allow filtering all tables in a database by related data in a
single table. For instance, if we want all data from every table that applies
to a subset of users we can create a Trace Filter that selects those users and
that data will be used to filter all other tables.

Trace filters work by creating a temporary table of the results of the filter
expression then joining each table to that if they contain a column listed in
`match_column`.

### Related Only

Related Only, the `related_only` table configuration, causes a table to be
filtered by the data in another table that may have itself been filtered.

For instance, you have a blog system and you want all posts from a specific
user and plus the comments. The posts have a user_id and are easily filtered.
But comments only have a post_id. A related_only configuration on the
`comments` table would specify the `posts` table, its `id` column, and a
foreign_column of `post_id`. This info can then be used to join to the filtered
`posts` table and we'll only get comments that are related to the selected
posts. This also works with Trace Filters applied to the related table.
