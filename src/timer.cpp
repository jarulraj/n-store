#include <iostream>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>

int log_enable = 1;

void runner(){
    for(int i=0 ; i<5 ; i++){
        std::cout<<i<<std::endl;
        sleep(1);
    }
}

void print(const boost::system::error_code& /*e*/, boost::asio::deadline_timer* t){
    std::cout << "Hi" << "\n";
    t->expires_at(t->expires_at() + boost::posix_time::seconds(1));
    t->async_wait(boost::bind(print, boost::asio::placeholders::error, t));
}

void logger(){
    if(log_enable){
        boost::asio::io_service io;
        boost::asio::deadline_timer t(io, boost::posix_time::seconds(1));

        t.async_wait(boost::bind(print, boost::asio::placeholders::error, &t));
        io.run();
    }
}

int main(){
    boost::thread_group logger_thread;
    logger_thread.create_thread(boost::bind(logger));

    boost::thread_group th_group;
    for(int i=0 ; i<2 ; i++)
        th_group.create_thread(boost::bind(runner));

    th_group.join_all();

    log_enable = 0;

    std::cout << "End \n";

    return 0;
}
