#include <string>
#include <cstring>
#include <cstdio>
using namespace std;

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define FMT_BUF_LEN 25 // double 24 bytes, int64_t 20 bytes
#define FMT_APPEND(args...) \
    lines_.resize(lines_.length() + FMT_BUF_LEN);\
    lines_.resize(lines_.length() - FMT_BUF_LEN + snprintf(&lines_[lines_.length() - FMT_BUF_LEN], FMT_BUF_LEN, ##args));

namespace influxdb_cpp {
    struct server_info {
        string host_;
        int port_;
        string db_;
        string usr_;
        string pwd_;
        server_info(const string& host, int port, const string& db = "", const string& usr = "", const string& pwd = "")
            : host_(host), port_(port), db_(db), usr_(usr), pwd_(pwd)
        {
        }
    };
    namespace detail {
        struct meas_caller;
        struct tag_caller;
        struct field_caller;
        struct ts_caller;
        int http_request(const char*, const char*, const string&, const string&, const server_info&, string* = NULL);
        void url_encode(string&, const std::string&);
    }

    int query(string& resp, const string& query, const server_info& si) {
        string qs("&q=");
        detail::url_encode(qs, query);
        return detail::http_request("GET", "query", qs, "", si, &resp);
    }

    class builder
    {
    public:
        detail::tag_caller& meas(const string& m) {
            lines_.clear();
            lines_.reserve(0x100);
            return _m(m);
        }

    protected:
        detail::tag_caller& _m(const string& m) {
            _escape(m, ", ");
            return (detail::tag_caller&)*this;
        }
        detail::tag_caller& _t(const string& k, const string& v) {
            lines_ += ',';
            _escape(k, ",= ");
            lines_ += '=';
            _escape(v, ",= ");
            return (detail::tag_caller&)*this;
        }
        detail::field_caller& _f_s(char delim, const string& k, const string& v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += "=\"";
            _escape(v, "\"");
            lines_ += '\"';
            return (detail::field_caller&)*this;
        }    
        detail::field_caller& _f_i(char delim, const string& k, unsigned long long v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%lldi", (unsigned long long)v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_f(char delim, const string& k, double v, int prec) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%.*lf", prec, v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_b(char delim, const string& k, bool v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            lines_ += (v ? 't' : 'f');
            return (detail::field_caller&)*this;
        }
        detail::ts_caller& _ts(unsigned long long ts) {
            FMT_APPEND(" %lld", ts);
            return (detail::ts_caller&)*this;
        }
        int _post_http(const server_info& si) {
            return detail::http_request("POST", "write", "", lines_, si);
        }
        int _send_udp(const string& host, int port) {
            int sock;
            struct sockaddr_in addr;
            
            if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
                return -1;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            if((addr.sin_addr.s_addr = inet_addr(host.c_str())) == INADDR_NONE)
                return -2;

            return sendto(sock, &lines_[0], lines_.length(), 0, (struct sockaddr *)&addr, sizeof(addr)) < (int)lines_.length() ? -3 : 0;
        }
        void _escape(const string& src, const char* escape_seq) {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_of(escape_seq, start)) != string::npos) {
                lines_.append(src.c_str() + start, pos - start);
                lines_ += '\\';
                lines_ += src[pos];
                start = ++pos;
            }
            lines_.append(src.c_str() + start, src.length() - start);
        }

        string lines_;
    };
    
    namespace detail {
        struct field_helper : public builder
        {
            detail::field_caller& field(const string& k, const string& v)        { return builder::_f_s(' ', k, v); }
            detail::field_caller& field(const string& k, bool v)                 { return builder::_f_b(' ', k, v); }
            detail::field_caller& field(const string& k, short v)                { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, unsigned short v)       { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, int v)                  { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, unsigned int v)         { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, long v)                 { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, unsigned long v)        { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, long long v)            { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, unsigned long long v)   { return builder::_f_i(' ', k, v); }
            detail::field_caller& field(const string& k, double v, int prec = 2) { return builder::_f_f(' ', k, v, prec); }
        private:
            detail::tag_caller& meas(const string& m);
        };
        struct tag_caller : public field_helper
        {
            detail::tag_caller& tag(const string& k, const string& v)            { return builder::_t(k, v); }
        private:
            detail::tag_caller& meas(const string& m);
        };
        struct field_caller : public field_helper
        {
            detail::tag_caller& meas(const string& m)                            { lines_ += '\n'; return builder::_m(m); }
            detail::ts_caller& timestamp(unsigned long long ts)                  { return builder::_ts(ts); }
            int post_http(const server_info& si)                                 { return builder::_post_http(si); }
            int send_udp(const string& host, int port)                           { return builder::_send_udp(host, port); }
        };
        struct ts_caller : public builder
        {
            detail::tag_caller& meas(const string& m)                            { lines_ += '\n'; return builder::_m(m); }
            int post_http(const server_info& si)                                 { return builder::_post_http(si); }
            int send_udp(const string& host, int port)                           { return builder::_send_udp(host, port); }
        };

        unsigned char to_hex(unsigned char x) { return  x > 9 ? x + 55 : x + 48; }
        void url_encode(string& out, const std::string& src)
        {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_not_of("abcdefghijklmnopqrstuvwxyqABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~", start)) != string::npos) {
                out.append(src.c_str() + start, pos - start);
                if(src[pos] == ' ')
                    out += "+";
                else {
                    out += '%';  
                    out += to_hex((unsigned char)src[pos] >> 4);
                    out += to_hex((unsigned char)src[pos] & 0xF);
                }
                start = ++pos;
            }
            out.append(src.c_str() + start, src.length() - start);
        }

        int http_request(const char* method, const char* uri,
            const string& querystring, const string& body, const server_info& si, string* resp) {
            string header;
            struct iovec iv[2];
            struct sockaddr_in addr;
            int sock, ret_code = 0, content_length = 0, len = 0;
            char ch;
            bool chunked = false;
            
            if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
                return -1;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(si.port_);
            if((addr.sin_addr.s_addr = inet_addr(si.host_.c_str())) == INADDR_NONE)
                return -2;

            if(connect(sock, (struct sockaddr*)(&addr), sizeof(addr)) < 0)
                return -3;

            header.resize(len = 0x100);

            for(;;) {
                iv[0].iov_len = snprintf(&header[0], len,
                    "%s /%s?db=%s&u=%s&p=%s%s HTTP/1.1\r\nHost: %s\r\nContent-Length: %zd\r\n\r\n",
                    method, uri, si.db_.c_str(), si.usr_.c_str(), si.pwd_.c_str(),
                    querystring.c_str(), si.host_.c_str(), body.length());
                if((int)iv[0].iov_len > len)
                    header.resize(len *= 2);
                else
                    break;
            }
            iv[0].iov_base = &header[0];
            iv[1].iov_base = (void*)&body[0];
            iv[1].iov_len = body.length();

            if(writev(sock, iv, 2) < (int)(iv[0].iov_len + iv[1].iov_len)) {
                ret_code = -6;
                goto END;
            }

            iv[0].iov_len = len;

#define _NO_MORE() (len >= (int)iv[0].iov_len && \
    (iv[0].iov_len = recv(sock, &header[0], header.length(), len = 0)) == size_t(-1))
#define _GET_NEXT_CHAR() (ch = _NO_MORE() ? 0 : header[len++])
#define _LOOP_NEXT(statement) for(;;) { if(!(_GET_NEXT_CHAR())) { ret_code = -7; goto END; } statement }
#define _UNTIL(c) _LOOP_NEXT( if(ch == c) break; )
#define _GET_NUMBER(n) _LOOP_NEXT( if(ch >= '0' && ch <= '9') n = n * 10 + (ch - '0'); else break; )
#define _GET_CHUNKED_LEN(n, c) _LOOP_NEXT( if(ch >= '0' && ch <= '9') n = n * 16 + (ch - '0'); \
            else if(ch >= 'A' && ch <= 'F') n = n * 16 + (ch - 'A') + 10; \
            else if(ch >= 'a' && ch <= 'f') n = n * 16 + (ch - 'a') + 10; else {if(ch != c) { ret_code = -8; goto END; } break;} )
#define _(c) if((_GET_NEXT_CHAR()) != c) break;
#define __(c) if((_GET_NEXT_CHAR()) != c) { ret_code = -9; goto END; }

            _UNTIL(' ')_GET_NUMBER(ret_code)
            for(;;) {
                _UNTIL('\n')
                switch(_GET_NEXT_CHAR()) {
                    case 'C':_('o')_('n')_('t')_('e')_('n')_('t')_('-')
                        _('L')_('e')_('n')_('g')_('t')_('h')_(':')_(' ')
                        _GET_NUMBER(content_length)
                        break;
                    case 'T':_('r')_('a')_('n')_('s')_('f')_('e')_('r')_('-')
                        _('E')_('n')_('c')_('o')_('d')_('i')_('n')_('g')_(':')
                        _(' ')_('c')_('h')_('u')_('n')_('k')_('e')_('d')
                        chunked = true;
                        break;
                    case '\r':__('\n')
                        switch(chunked) {
                            do {__('\r')__('\n')
                            case true:
                                _GET_CHUNKED_LEN(content_length, '\r')__('\n')
                                if(!content_length) {
                                    __('\r')__('\n')
                                    goto END;
                                }
                            case false:
                                while(content_length > 0 && !_NO_MORE()) {
                                    content_length -= (iv[1].iov_len = min(content_length, (int)iv[0].iov_len - len));
                                    if(resp) resp->append(&header[len], iv[1].iov_len);
                                    len += iv[1].iov_len;
                                }
                            } while(chunked);
                        }
                        goto END;
                }
                if(!ch) {
                    ret_code = -10;
                    goto END;
                }
            }
            ret_code = -11;
        END:
            return ret_code / 100 == 2 ? 0 : ret_code;
#undef _NO_MORE
#undef _GET_NEXT_CHAR
#undef _LOOP_NEXT
#undef _UNTIL
#undef _GET_NUMBER
#undef _GET_CHUNKED_LEN
#undef _
#undef __
        }
    }
}

#undef FMT_BUF_LEN
#undef FMT_APPEND