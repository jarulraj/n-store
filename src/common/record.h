#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>

using namespace std;

#define DELIM ' '

class record{
    public:
        record(unsigned int _key, std::string _value) :
            key(_key),
            value(_value){}

        friend ostream& operator<<(ostream& out, const record& rec){
            out << DELIM << rec.key << DELIM << rec.value << DELIM;
            return out;
        }

        friend istream& operator>>(istream& in, record& rec){
            in.ignore(1); // skip delimiter
            in >> rec.key;

            in.ignore(1); // skip delimiter
            in >> rec.value;

            in.ignore(1); // skip delimiter
            return in;
        }


        //private:
        unsigned int key;
        std::string value;
};

class sp_record : public record  {
public:
	sp_record() :
			record(0, ""), location(NULL) {
	}

	sp_record(unsigned int _key, std::string _value, char* _location) :
			record(_key, _value), location(_location) {
	}

	//private:
	char* location;
};


#endif /* RECORD_H_ */
