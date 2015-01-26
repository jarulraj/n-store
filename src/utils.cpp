#include <algorithm>
#include <random>
#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cassert>

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#include "utils.h"
#include "record.h"

namespace storage {

    // RAND GEN
    std::string get_rand_astring(size_t len) {
        static const char alphanum[] = "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        char rep = alphanum[rand() % (sizeof(alphanum) - 1)];
        std::string str(len, rep);
        return str;
    }

    double get_rand_double(double d_min, double d_max) {
        double f = (double) rand() / RAND_MAX;
        return d_min + f * (d_max - d_min);
    }

    bool get_rand_bool(double ratio) {
        double f = (double) rand() / RAND_MAX;
        return (f < ratio) ? true : false;
    }

    int get_rand_int(int i_min, int i_max) {
        double f = (double) rand() / RAND_MAX;
        return i_min + (int) (f * (double) (i_max - i_min));
    }

    int get_rand_int_excluding(int i_min, int i_max, int excl) {
        int ret;
        double f;

        if (i_max == i_min + 1) {
            if (excl == i_max)
                return i_min;
            else
                return i_max;
        }

        while (1) {
            f = (double) rand() / RAND_MAX;
            ret = i_min + (int) (f * (double) (i_max - i_min));
            if (ret != excl)
                break;
        }

        return ret;
    }

    std::string get_tuple(std::stringstream& entry, schema* sptr) {
        if (sptr == NULL)
            return "";

        std::string tuple, field;

        for (unsigned int col = 0; col < sptr->num_columns; col++) {
            entry >> field;
            tuple += field + " ";
        }

        return tuple;
    }

    // TIMER

    void display_stats(engine_type etype, double duration, int num_txns) {
        double throughput;

        switch (etype) {
            case engine_type::WAL:
                std::cout << "WAL :: ";
                break;

            case engine_type::SP:
                std::cout << "SP :: ";
                break;

            case engine_type::LSM:
                std::cout << "LSM :: ";
                break;

            case engine_type::OPT_WAL:
                std::cout << "OPT_WAL :: ";
                break;

            case engine_type::OPT_SP:
                std::cout << "OPT_SP :: ";
                break;

            case engine_type::OPT_LSM:
                std::cout << "OPT_LSM :: ";
                break;

            default:
                std::cout << "Unknown engine type " << std::endl;
                break;
        }

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Duration(s) : " << (duration / 1000.0) << " ";

        throughput = (num_txns * 1000.0) / duration;
        std::cout << "Throughput  : " << throughput << std::endl;
    }

    // RANDOM DIST
    void zipf(std::vector<int>& zipf_dist, double alpha, int n, int num_values) {
        static double c = 0;          // Normalization constant
        double z;          // Uniform random number (0 < z < 1)
        double sum_prob;          // Sum of probabilities
        double zipf_value = 0;          // Computed exponential value to be returned
        int i, j;

        double* powers = new double[n + 1];
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0.001, 0.099);

        for (i = 1; i <= n; i++)
            powers[i] = 1.0 / pow((double) i, alpha);

        // Compute normalization constant on first call only
        for (i = 1; i <= n; i++)
            c = c + powers[i];
        c = 1.0 / c;

        for (i = 1; i <= n; i++)
            powers[i] = c * powers[i];

        for (j = 1; j <= num_values; j++) {

            // Pull a uniform random number in (0,1)
            z = dis(gen);

            // Map z to the value
            sum_prob = 0;
            for (i = 1; i <= n; i++) {
                sum_prob = sum_prob + powers[i];
                if (sum_prob >= z) {
                    zipf_value = i;
                    break;
                }
            }

            //std::cout << "zipf_val :: " << zipf_value << std::endl;
            zipf_dist.push_back(zipf_value);
        }

        delete[] powers;
    }

    // Simple skew generator
    void simple_skew(std::vector<int>& simple_dist, double alpha, int n,
            int num_values) {
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0, 1);
        double i, z;
        long bound = 10;  // alpha fraction goes to skewed values
        long val, diff;

        diff = n - bound;

        for (i = 0; i < num_values; i++) {
            z = dis(gen);

            if (z < alpha)
                val = z * bound;
            else
                val = bound + z * diff;

            //std::cout << "simple_val :: " << val << " bound : "<<bound<<" alpha : "<<alpha<< " z : "<<z<<std::endl;
            simple_dist.push_back(val);
        }
    }

    void uniform(std::vector<double>& uniform_dist, int num_values) {
        int seed = 0;
        std::mt19937 gen(seed);
        std::uniform_real_distribution<> dis(0, 1);
        double i;

        for (i = 0; i < num_values; i++)
            uniform_dist.push_back(dis(gen));
    }

    // PTHREAD WRAPPERS

    void wrlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_wrlock(access);
        if (rc != 0) {
            perror("wrlock failed \n");
            exit(EXIT_FAILURE);
        }
    }

    void rdlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_rdlock(access);
        if (rc != 0) {
            perror("rdlock failed \n");
            exit(EXIT_FAILURE);
        }
    }

    int try_wrlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_trywrlock(access);
        if (rc != 0) {
            perror("trywrlock failed \n");
            exit(EXIT_FAILURE);
        }

        return rc;
    }

    int try_rdlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_tryrdlock(access);
        if (rc != 0) {
            perror("tryrdlock failed \n");
        }

        return rc;
    }

    void unlock(pthread_rwlock_t* access) {
        int rc = pthread_rwlock_unlock(access);
        if (rc != 0) {
            perror("unlock failed \n");
        }
    }

    // PCOMMIT helpers

#define CPU_FREQ_MHZ (2593)

    static inline unsigned long read_tsc(void){
        unsigned long var;
        unsigned int hi, lo;

        asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        var = ((unsigned long long int) hi << 32) | lo;

        return var;
    }

    static inline void cpu_pause(){
        __asm__ volatile ("pause" ::: "memory");
    }

    static inline void clwb(){
        cpu_pause();
    }

    void pcommit(unsigned long lat){
        unsigned long etsc = read_tsc() + (unsigned long)(lat*CPU_FREQ_MHZ/1000);
        while (read_tsc() < etsc)
            cpu_pause();
    }

#define DELAY_IN_NS (800)

    static inline void test_pcommit(){
        unsigned long stsc, etsc;

        stsc = read_tsc();
        pcommit(DELAY_IN_NS);
        etsc = read_tsc();
        printf ("pm_wbarrier latency: %lu ns\n",
                (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

        stsc = read_tsc();
        pcommit(2*DELAY_IN_NS);
        etsc = read_tsc();
        printf ("pm_wbarrier latency: %lu ns\n",
                (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

        stsc = read_tsc();
        clwb();
        etsc = read_tsc();
        printf ("clwb latency: %lu ns\n",
                (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));
    }

}

