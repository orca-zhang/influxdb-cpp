#include <string>
#include <cstring>
#include <cstdio>

#ifdef _WIN32
    #define NOMINMAX
    #include <windows.h>
    #include <algorithm>
    #pragma comment(lib, "ws2_32")
    #define close closesocket
    typedef struct iovec { void* iov_base; size_t iov_len; } iovec;
    inline __int64 writev(int sock, struct iovec* iov, int cnt) {
        __int64 r = send(sock, (const char*)iov->iov_base, iov->iov_len, 0);
        return (r < 0 || cnt == 1) ? r : r + writev(sock, iov + 1, cnt - 1);
    }
#else
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
#endif

#define FMT_BUF_LEN 25 // double 24 bytes, int64_t 21 bytes
#define FMT_APPEND(...) \
    lines_.resize(lines_.length() + FMT_BUF_LEN);\
    lines_.resize(lines_.length() - FMT_BUF_LEN + snprintf(&lines_[lines_.length() - FMT_BUF_LEN], FMT_BUF_LEN, ##__VA_ARGS__));

namespace influxdb_cpp {
    struct server_info {
        std::string host_;
        int port_;
        std::string db_;
        std::string usr_;
        std::string pwd_;
        server_info(const std::string& host, int port, const std::string& db = "", const std::string& usr = "", const std::string& pwd = "")
            : host_(host), port_(port), db_(db), usr_(usr), pwd_(pwd) {}
    };
    namespace detail {
        struct meas_caller;
        struct tag_caller;
        struct field_caller;
        struct ts_caller;
        int http_request(const char*, const char*, const std::string&, const std::string&, const server_info&, std::string*);
        unsigned char to_hex(unsigned char x) { return  x > 9 ? x + 55 : x + 48; }
        void url_encode(std::string& out, const std::string& src) {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_not_of("abcdefghijklmnopqrstuvwxyqABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~", start)) != std::string::npos) {
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
    }

    int query(std::string& resp, const std::string& query, const server_info& si) {
        std::string qs("&q=");
        detail::url_encode(qs, query);
        return detail::http_request("GET", "query", qs, "", si, &resp);
    }
    int create_db(std::string& resp, const std::string& db_name, const server_info& si) {
        std::string qs("&q=create+database+");
        detail::url_encode(qs, db_name);
        return detail::http_request("POST", "query", qs, "", si, &resp);
    }

    struct builder {
        detail::tag_caller& meas(const std::string& m) {
            lines_.clear();
            lines_.reserve(0x100);
            return _m(m);
        }
    protected:
        detail::tag_caller& _m(const std::string& m) {
            _escape(m, ", ");
            return (detail::tag_caller&)*this;
        }
        detail::tag_caller& _t(const std::string& k, const std::string& v) {
            lines_ += ',';
            _escape(k, ",= ");
            lines_ += '=';
            _escape(v, ",= ");
            return (detail::tag_caller&)*this;
        }
        detail::field_caller& _f_s(char delim, const std::string& k, const std::string& v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += "=\"";
            _escape(v, "\"");
            lines_ += '\"';
            return (detail::field_caller&)*this;
        }    
        detail::field_caller& _f_i(char delim, const std::string& k, long long v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%lldi", v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_f(char delim, const std::string& k, double v, int prec) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%.*lf", prec, v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_b(char delim, const std::string& k, bool v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            lines_ += (v ? 't' : 'f');
            return (detail::field_caller&)*this;
        }
        detail::ts_caller& _ts(long long ts) {
            FMT_APPEND(" %lld", ts);
            return (detail::ts_caller&)*this;
        }
        int _post_http(const server_info& si, std::string* resp) {
            return detail::http_request("POST", "write", "", lines_, si, resp);
        }
        int _send_udp(const std::string& host, int port) {
            int sock, ret = 0;
            struct sockaddr_in addr;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            if((addr.sin_addr.s_addr = inet_addr(host.c_str())) == INADDR_NONE)
                return -1;
            
            if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
                return -2;

            if(sendto(sock, &lines_[0], lines_.length(), 0, (struct sockaddr *)&addr, sizeof(addr)) < (int)lines_.length())
                ret = -3;

            close(sock);
            return ret;
        }
        void _escape(const std::string& src, const char* escape_seq) {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_of(escape_seq, start)) != std::string::npos) {
                lines_.append(src.c_str() + start, pos - start);
                lines_ += '\\';
                lines_ += src[pos];
                start = ++pos;
            }
            lines_.append(src.c_str() + start, src.length() - start);
        }

        std::string lines_;
    };
    
    namespace detail {
        struct tag_caller : public builder {
            detail::tag_caller& tag(const std::string& k, const std::string& v)       { return _t(k, v); }
            detail::field_caller& field(const std::string& k, const std::string& v)   { return _f_s(' ', k, v); }
            detail::field_caller& field(const std::string& k, bool v)                 { return _f_b(' ', k, v); }
            detail::field_caller& field(const std::string& k, short v)                { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, int v)                  { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, long v)                 { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, long long v)            { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, double v, int prec = 2) { return _f_f(' ', k, v, prec); }
        private:
            detail::tag_caller& meas(const std::string& m);
        };
        struct ts_caller : public builder {
            detail::tag_caller& meas(const std::string& m)                            { lines_ += '\n'; return _m(m); }
            int post_http(const server_info& si, std::string* resp = NULL)            { return _post_http(si, resp); }
            int send_udp(const std::string& host, int port)                           { return _send_udp(host, port); }
        };
        struct field_caller : public ts_caller {
            detail::field_caller& field(const std::string& k, const std::string& v)   { return _f_s(',', k, v); }
            detail::field_caller& field(const std::string& k, bool v)                 { return _f_b(',', k, v); }
            detail::field_caller& field(const std::string& k, short v)                { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, int v)                  { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, long v)                 { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, long long v)            { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, double v, int prec = 2) { return _f_f(',', k, v, prec); }
            detail::ts_caller& timestamp(unsigned long long ts)                  { return _ts(ts); }
        };
        int http_request(const char* method, const char* uri,
            const std::string& querystring, const std::string& body, const server_info& si, std::string* resp) {
            std::string header;
            struct iovec iv[2];
            struct sockaddr_in addr;
            int sock, ret_code = 0, content_length = 0, len = 0;
            char ch;
            bool chunked = false;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(si.port_);
            if((addr.sin_addr.s_addr = inet_addr(si.host_.c_str())) == INADDR_NONE)
                return -1;

            if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
                return -2;

            if(connect(sock, (struct sockaddr*)(&addr), sizeof(addr)) < 0) {
                close(sock);
                return -3;
            }

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

            if(resp) resp->clear();

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
                                    content_length -= (iv[1].iov_len = std::min(content_length, (int)iv[0].iov_len - len));
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
            close(sock);
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