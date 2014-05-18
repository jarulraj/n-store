#include <iostream>
#include <map>
#include <sstream>
#include <fstream>

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/map.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using namespace std;

int main()
{
    std::map<int, int> map = {{1,2}, {2,1}};
    std::ofstream ofile("map");
    
    boost::archive::text_oarchive oarch(ofile, std::ofstream::out);
    oarch << map;

    ofile.close();

    std::map<int, int> new_map;
    std::ifstream ifile("map");

    boost::archive::text_iarchive iarch(ifile);
    iarch >> new_map;
    
    std::cout << (map == new_map) << std::endl;

}
