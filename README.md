# influxdb-cpp

### What is influxdb-cpp?

[![license](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat)](https://github.com/orca-zhang/influxdb-cpp/blob/master/LICENSE)

- A header-only query & write client for C++.

### Why use influxdb-cpp?

- **Exact-small**: less than 300 lines and only 10KB+.
- **Easy-to-use**: it's designed to be used without extra studies.
- **Easy-to-assemble**: only a tiny header file needs to be included.
- **No-dependencies**: unless STL and std c libraries.

### Examples

#### before using

- The very simple thing you should do before using is only:

    ```cpp
    #include "influxdb.hpp"
    ```

#### write example

- You should according to the [write syntax(v1.4)](https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_reference/) while writing series(metrics).

    ```
    measurement[,tag-key=tag-value...] field-key=field-value[,field2-key=field2-value...] [unix-nano-timestamp]
    ```


- You can rapidly start writing serires by according to the following example.

    ```cpp
    influxdb_cpp::server_info si("127.0.0.1", 8086, "db", "usr", "pwd");
    influxdb_cpp::builder()
        .meas("foo")
        .tag("k", "v")
        .tag("x", "y")
        .field("x", 10)
        .field("y", 10.3, 2)
        .field("y", 10.3456)
        .field("b", !!10)
        .timestamp(1521841498234)
        .post_http(si);
    ```

- The series to be send is like:

    ```
    foo,k=v,x=y x=10i y=10.30 y=10.35 b=t 1521841498234
    ```

- **NOTE**: 
  - 3rd parameter of `field()`: `precision` is optional for floating field value, and default precision is `2`. 
  - `usr` and `pwd` is optional for authorization.

- You could change `post_http` to `send_udp` for udp request. only `host` and `port` is required for udp.

    ```cpp
    influxdb_cpp::server_info si("127.0.0.1", 8091);
    influxdb_cpp::builder()
        .meas("foo")
        .field("x", 10)
        .send_udp(si);
    ```

- Bulk/batch is also support. 

    ```cpp
    influxdb_cpp::builder()
        .meas("foo")
        .field("x", 10)
        .meas("bar")
        .field("y", 10.3)
        .send_udp(si);
    ```

- The series to be send is like:

    ```
    foo x=10i
    bar y=10.30
    ```

#### query example

- And you can query series by according to the following example.

    ```cpp
    influxdb_cpp::server_info si("127.0.0.1", 8086, "db", "usr", "pwd");

    string resp;
    influxdb_cpp::query(resp, "select * from t", si);
    ```

### TODO

- Add more test cases.
- Supports DSN initializatin for server_info.
- Add query response parse tutorial.

### Misc

- Please feel free to use influxdb-cpp.
- Looking forward to your suggestions.
- If your project is using influxdb-cpp, you can show your project or company here by creating a issue or let we know.