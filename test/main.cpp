#include "../influxdb.hpp"
#include <iostream>
using namespace std;

// Visit https://github.com/orca-zhang/influxdb-c for more test cases

int main(int argc, char const *argv[])
{
    influxdb_cpp::server_info si("127.0.0.1", 8086, "testx", "test", "test");
    // post_http demo with resp[optional]
    string resp;
    int ret = influxdb_cpp::builder()
        .meas("test")
        .tag("k", "v")
        .tag("x", "y")
        .field("x", 10)
        .field("y", 10.3, 2)
        .field("b", !!10)
        .timestamp(1512722735522840439)
        .post_http(si, &resp);

    cout << ret << endl << resp << endl;

    // send_udp demo
    ret = influxdb_cpp::builder()
        .meas("test")
        .tag("k", "v")
        .tag("x", "y")
        .field("x", 10)
        .field("y", 3.14e18, 3)
        .field("b", !!10)
        .timestamp(1512722735522840439)
        .send_udp("127.0.0.1", 8089);

    cout << ret << endl;

    // query from table
    influxdb_cpp::server_info si_new("127.0.0.1", 8086, "", "test", "test");
    influxdb_cpp::query(resp, "show databases", si);
    cout << resp << endl;

    // create_db
    influxdb_cpp::create_db(resp, "x", si_new);
    cout << resp << endl;
    return 0;
}
