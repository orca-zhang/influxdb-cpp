#include "../influxdb.hpp"
#include <iostream>
using namespace std;

int main(int argc, char const *argv[])
{
    influxdb_cpp::server_info si("127.0.0.1", 8086, "test", "test", "test");
    influxdb_cpp::builder()
        .meas("test")
        .tag("k", "v")
        .tag("x", "y")
        .field_i("x", 10)
        .field_f("y", 10.3, 2)
        .field_b("b", 10)
        .timestamp(1521841498234)
        .post_http(si);

    string resp;
    influxdb_cpp::query(resp, "select * from t", si);
    cout << resp << endl;
    return 0;
}