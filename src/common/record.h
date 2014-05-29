#ifndef RECORD_H_
#define RECORD_H_

#include <iostream>

using namespace std;

#define DELIM ' '

class record{
    public:
        record(unsigned int _key, char* _value) :
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

        char* to_string(){
			char *bp;
			size_t buffer_size;
			FILE* buffer_stream;

			buffer_stream = open_memstream(&bp, &buffer_size);

			fprintf(buffer_stream, "%c%d%c%s%c", DELIM, key, DELIM, value, DELIM);
			fclose (buffer_stream);

			//printf("record:: %s %lu \n", bp, buffer_size);

			return bp;
        }


        //private:
        unsigned int key;
        char* value;
};

class sp_record {
public:
	sp_record() :
			key(0), location { NULL, NULL } {
	}

	sp_record(unsigned int _key, unsigned int _batch_id, char* _location) :
			key(_key), batch_id { _batch_id, 0 }, location { _location, NULL } {
	}

	//private:
	unsigned int key;
	unsigned int batch_id[2];
	char* location[2];
};


#endif /* RECORD_H_ */
