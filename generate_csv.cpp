#include <iostream>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <random>
#include <ctime>
#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <algorithm>

struct DataType {
    std::string type;
    int length;    // Used for VARCHAR
    int precision; // Used for DECIMAL
    int scale;     // Used for DECIMAL
    DataType() : type(), length(0), precision(16), scale(6) {}
    DataType(std::string t, int len = 0, int prec = 0, int sc = 0)
        : type(std::move(t)), length(len), precision(prec), scale(sc) {}
};

long long gKey = 0;
long long gRKey = 0;

std::random_device rd;
std::mt19937 gen(rd());

std::string generate_bit() {
    std::uniform_int_distribution<> dist(0, 1);
    return std::to_string(dist(gen));
}

template <typename T>
std::string generate_integer_type(T min, T max) {
    std::uniform_int_distribution<T> dist(min, max);
    return std::to_string(dist(gen));
}

std::string generate_float(float min, float max) {
    std::uniform_real_distribution<float> dist(min, max);
    std::stringstream ss;
    ss << dist(gen);
    return ss.str();
}

std::string generate_double(double min, double max) {
    std::uniform_real_distribution<double> dist(min, max);
    std::stringstream ss;
    ss << dist(gen);
    return ss.str();
}

std::string generate_char(size_t length) {
    std::uniform_int_distribution<> dist(65, 90); // ASCII A-Z
    std::string str(length, ' ');
    for (size_t i = 0; i < length; ++i) {
        str[i] = static_cast<char>(dist(gen));
    }
    return str;
}

std::string generate_varchar(size_t length) {
    static const char charset[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
    std::string str(length, ' ');
    for (size_t i = 0; i < length; ++i) {
        str[i] = charset[dist(gen)];
    }
    return str;
}

std::string generate_text() {
    // For simplicity, let's generate text with random length up to 256 characters
    std::uniform_int_distribution<> length_dist(1, 256);
    size_t length = length_dist(gen);
    return generate_varchar(length);
}

std::string generate_blob() {
    // Let's generate a BLOB with random length up to 256 bytes
    std::uniform_int_distribution<> length_dist(1, 256);
    size_t length = length_dist(gen);
    return generate_varchar(length);
}

std::string generate_datetime() {
    std::uniform_int_distribution<> year_dist(1900, 2021);
    std::uniform_int_distribution<> month_dist(1, 12);
    std::uniform_int_distribution<> day_dist(1, 28); // Simple day generator
    std::uniform_int_distribution<> hour_dist(0, 23);
    std::uniform_int_distribution<> minute_dist(0, 59);
    std::uniform_int_distribution<> second_dist(0, 59);

    std::stringstream datetime;
    datetime << year_dist(gen) << "-"
             << std::setfill('0') << std::setw(2) << month_dist(gen) << "-"
             << std::setfill('0') << std::setw(2) << day_dist(gen) << " "
             << std::setfill('0') << std::setw(2) << hour_dist(gen) << ":"
             << std::setfill('0') << std::setw(2) << minute_dist(gen) << ":"
             << std::setfill('0') << std::setw(2) << second_dist(gen);
    return datetime.str();
}

std::string generate_date() {
    return generate_datetime().substr(0, 10); // Reuse datetime generator for date
}

std::string generate_time() {
    std::uniform_int_distribution<> hour_dist(0, 23);
    std::uniform_int_distribution<> minute_dist(0, 59);
    std::uniform_int_distribution<> second_dist(0, 59);

    std::stringstream time;
    time << std::setfill('0') << std::setw(2) << hour_dist(gen) << ":"
         << std::setfill('0') << std::setw(2) << minute_dist(gen) << ":"
         << std::setfill('0') << std::setw(2) << second_dist(gen);
    return time.str();
}

std::string generate_timestamp() {
    // Assuming the current epoch time range supported by MySQL
    std::uniform_int_distribution<int64_t> dist(1, static_cast<int64_t>(time(nullptr)));
    time_t timestamp = dist(gen);

    char buffer[20]; // Buffer to hold the timestamp string
    strftime(buffer, sizeof(buffer), "%F %T", std::gmtime(&timestamp));
    return std::string(buffer);
}

std::string generate_year() {
    std::uniform_int_distribution<> year_dist(1901, 2155); // YEAR datatype range in MySQL
    std::stringstream year;
    year << year_dist(gen);
    return year.str();
}

std::string generate_decimal(int precision, int scale) {
    std::uniform_int_distribution<int64_t> int_dist(0, static_cast<int>(pow(10, precision - scale) - 1));
    std::uniform_int_distribution<int64_t> frac_dist(0, static_cast<int>(pow(10, scale) - 1));
    std::stringstream decimal;
    decimal << int_dist(gen) << "." << std::setw(scale) << std::setfill('0') << frac_dist(gen);
    return decimal.str();
}

uint64_t bit_permute_step(uint64_t x, uint64_t m, unsigned shift) {
  uint64_t t;
  t = ((x >> shift) ^ x) & m;
  x = (x ^ t) ^ (t << shift);
  return x;
}
uint64_t segregate4(uint64_t x)
{ // generated by http://programming.sirrida.de/calcperm.php, extended to 64-bit
  x = bit_permute_step(x, 0x2222222222222222ull, 1);
  x = bit_permute_step(x, 0x0c0c0c0c0c0c0c0cull, 2);
  x = bit_permute_step(x, 0x00f000f000f000f0ull, 4);
  return x;
}

void generate_csv(const std::vector<DataType>& types, size_t rows) {
    std::ostream &file = std::cout;
    for (size_t i = 0; i < rows; ++i) {
#if 0
        if (i % 100000 == 0) {
          std::cout << i << std::endl;
        }
#endif

/*
    std::cout << "Random BIT: " << generate_bit() << std::endl;
    std::cout << "Random TINYINT: " << generate_integer_type<int8_t>(-128, 127) << std::endl;
    std::cout << "Random SMALLINT: " << generate_integer_type<int16_t>(-32768, 32767) << std::endl;
    std::cout << "Random MEDIUMINT: " << generate_integer_type<int32_t>(-8388608, 8388607) << std::endl;
    std::cout << "Random INT: " << generate_integer_type<int32_t>(-2147483648, 2147483647) << std::endl;
    std::cout << "Random BIGINT: " << generate_integer_type<int64_t>(INT64_MIN, INT64_MAX) << std::endl;
    std::cout << "Random FLOAT: " << generate_float(-1e38f, 1e38f) << std::endl;
    std::cout << "Random DOUBLE: " << generate_double(-1e308, 1e308) << std::endl;
    std::cout << "Random CHAR(5): " << generate_char(5) << std::endl;
    std::cout << "Random VARCHAR(10): " << generate_varchar(10) << std::endl;
    std::cout << "Random TEXT: " << generate_text() << std::endl;
    std::cout << "Random BLOB: " << generate_blob() << std::endl;
    std::cout << "Random DATE: " << generate_date() << std::endl;
    std::cout << "Random DATETIME: " << generate_datetime() << std::endl;
    std::cout << "Random TIMESTAMP: " << generate_timestamp() << std::endl;
    std::cout << "Random TIME: " << generate_time() << std::endl;
    std::cout << "Random YEAR: " << generate_year() << std::endl;
    std::cout << "Random DECIMAL(10, 4): " << generate_decimal(10, 4) << std::endl;
*/
        for (size_t j = 0; j < types.size(); ++j) {
            // file << types[j].type << ":";
            if (j > 0) {
                file << ",";
            }
            if (types[j].type == "bit") {
                file << generate_bit();
            } else if (types[j].type == "key") {
                file << gKey++;
            } else if (types[j].type == "rkey") {
                gRKey++;
                uint64_t value = ((gRKey << 63) | (gRKey >> 1));
                file << segregate4(value);
            } else if (types[j].type == "tinyint") {
                file << generate_integer_type<int8_t>(-128, 127);
            } else if (types[j].type == "smallint") {
                file << generate_integer_type<int16_t>(-32768, 32767);
            } else if (types[j].type == "mediumint") {
                file << generate_integer_type<int32_t>(-8388608, 8388607);
            } else if (types[j].type == "int") {
                file << generate_integer_type<int32_t>(-2147483648, 2147483647);
            } else if (types[j].type == "bigint") {
                file << generate_integer_type<int64_t>(INT64_MIN, INT64_MAX);
            } else if (types[j].type == "float") {
                file << generate_float(-1e38f, 1e38f);
            } else if (types[j].type == "double") {
                file << generate_double(-2147483648, 2147483647);
            } else if (types[j].type == "char" || types[j].type == "nchar")  {
                file << generate_char(types[j].length);
            } else if (types[j].type == "varchar" || types[j].type == "varchar2" || types[j].type == "nvarchar2") {
                file << generate_varchar(types[j].length);
            } else if (types[j].type == "text") {
                file << generate_text();
            } else if (types[j].type == "blob") {
                file << generate_blob();
            } else if (types[j].type == "date") {
                file << generate_date();
            } else if (types[j].type == "datetime") {
                file << generate_datetime();
            } else if (types[j].type == "timestamp") {
                file << generate_timestamp();
            } else if (types[j].type == "time") {
                file << generate_time();
            } else if (types[j].type == "year") {
                file << generate_year();
            } else if (types[j].type == "decimal" || types[j].type == "number") {
                file << generate_decimal(types[j].precision, types[j].scale);
            } else {
                // unknown type, output nothing for this column
            }
        }
        file << "\n";
    }
}

void expectSpace(const std::string& input,
        std::string::size_type &pos)
{
    while(pos < input.length() && input[pos] == ' ') pos++;
}

void expectInt(const std::string& input,
        std::string::size_type &pos,
        int &num)
{
    if (pos >= input.length()) {
        throw "Unexpected EOL";
    }
    //std::cout << "expectInt" << std::endl;
    std::string::size_type start = pos;
    while(pos < input.length()) {
        if (isdigit(input[pos])) {
            pos++;
        } else {
            break;
        }
    }
    if (pos == start) {
        throw "No int value found in (...)";
    }
    std::string numStr = input.substr(start, pos - start);
    num = std::stoi(numStr);
    //std::cout << "expectInt values" << numStr << " " << num << std::endl;
}

void expectType(const std::string& input,
        std::string::size_type &pos,
        std::string &type)
{
    if (pos >= input.length()) {
        throw "Unexpected EOL";
    }
    //std::cout << "expectType" << std::endl;
    std::string::size_type start = pos;
    while(pos < input.length()) {
        if (isalpha(input[pos]) || isdigit(input[pos])) {
            pos++;
        } else {
            break;
        }
    }
    if (pos == start) {
        std::cout << "Parsing " << input.substr(start, input.length() - start) << std::endl;
        throw "No type found while parsing";
    }
    type = input.substr(start, pos - start);
    //std::cout << type << std::endl;
}


void expectElement(const std::string& input,
        std::string::size_type &pos,
        DataType &type)
{
    expectSpace(input, pos);
    if (pos >= input.length()) {
        throw "Unexpected EOL";
    }
    //std::cout << "expectElement" << std::endl;
    expectType(input, pos, type.type);
    if (input[pos] == '(') {
        pos++;
        expectInt(input, pos, type.length);
        expectSpace(input, pos);
        if (input[pos] == ',') {
            pos++;
            expectSpace(input, pos);
            type.precision = type.length;
            type.length = 0;
            expectInt(input, pos, type.scale);
            expectSpace(input, pos);
            if (type.precision < type.scale)
            {
                type.precision = type.scale;
            }
        }
        if (input[pos] != ')') {
            throw "Unterminated bracket!";
        }
        pos++;
    }
    expectSpace(input, pos);
}

// input = element,element,...
// element=type|type(num)|type(num,num)
// num=int
// type=token
std::vector<DataType> parseDataTypes(const std::string& input) {
    std::vector<DataType> dataTypes;
    std::string::size_type startPos = 0;
    std::string::size_type curPos = 0;
    while (curPos < input.length()) {
        DataType type;
        expectElement(input, curPos, type);
        dataTypes.push_back(type);
        curPos++;
    }
    return dataTypes;
}



int main(int argc, char **argv) {
    const char *columns;
    long long rows = 0;
    if (argc != 3) {
      rows = 100000;
      columns = "int,varchar(50),double,date,bigint";
    } else {
      rows = atoll(argv[1]);
      columns = argv[2];
    }
    std::string input = columns;
    transform(input.begin(),input.end(),input.begin(),::tolower);
    try {
        std::vector<DataType> types = parseDataTypes(input);
        generate_csv(types, rows);
    } catch (char const * msg) {
        std::cout << msg << std::endl;
    } catch(std::exception& e) {
        std::cout << "Unknown expection parsing data"  << std::endl;
    }
    return 0;
}
