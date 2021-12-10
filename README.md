# fdb-ch-proto-export

A command-line tool to export FoundationDB stored Protocol buffers to ClickhouseDB.

## Installation

## Usage

```sh-session
fdb-ch [command]

fdb-ch [command] help
```

### Mapping file

The mapping file is used to bind proto messages to table definitions as well
as specifying the range of keys to export.

```json
[
  {
    "from": "users",
    "to": "users\\xFF",
    "proto": "protos.User",
    "table": "default.users"
  }
]
```

## Commands

- [`setup`](#setup)
- [`export`](#export)

### Setup

#### View the current config setup

```sh-session
fdb-ch setup view
```

#### Set up fdb cluster file

```sh-session
fdb-ch setup set --cluster-file /etc/foundationdb/fdb.cluster
```

#### Set up clickhouse url

```sh-session
fdb-ch setup set --clickhouse-url http://localhost:8083
```

#### Set up proto file path

```sh-session
fdb-ch setup set --proto-file ~/demo.proto
```

#### Set up mapping file

```sh-session
fdb-ch setup set --mapping-file ~/mapping.json
```

### Export

Export with logs on

```sh-session
RUST_LOG=info fdb-ch export
```

## Currently known to be unsupported

- A few unsupported proto types
- Edge cases with nested objects
