# spkeeper

## About

A utility for versioning SQL Server stored procedures.

Given a database name and credentials for accessing it, will save all stored procedures to a local directory which will also be initialised with a git repository.

When run again on the same database, any changes that have been made to the database's stored procedures will be committed to git.

## Requirements

[libgit2][libgit2] must be available and your system must be configured such that go binaries can dynamically link against it.

[FreeTDS][freetds] must be installed and configured for the databases you wish to use spkeeper with.

## Building

With the requirements in place, spkeeper should be buildable by cloning this repository and running `go install`.

## Usage

spkeeper can be used as follows:

```
$ spkeeper -h 127.0.0.1 -d database_name -u db_username -p db_password -o /path/to/save/to -n "Git Username" -"git.email@example.com"
```

The options are described in the table below.

### Options

Option | Req. | Description| Default
-------|------|------------|---------
h      | n    | Database host name | 127.0.0.1
d      | y    | Database name      |
u      | n    | Database user name | sa
p      | y    | Database password  | 
o      | y    | Output path        |
n      | n    | Name to use for git commits | spkeeper
e      | n    | Email address to use for git commits | spkeeper@example.com

## Output

When run against a database, spkeeper will create a subdirectory named after the database inside the provided output path. Inside this directory will be a set of `.sql` files, one per stored procedure found in the database.

The output path itself will have a git repository initialised inside it (if one does not already exist), and all of the `.sql` files will be committed to the `master` branch.

## To Do

- [ ] Handle stored procedures being deleted.
- [ ] Allow control over which git branch is used.

[freetds]: http://www.freetds.org/
[libgit2]: https://libgit2.github.com