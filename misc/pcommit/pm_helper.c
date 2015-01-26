#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#define CPU_FREQ_MHZ (2593)

static inline unsigned long read_tsc(void)
{
    unsigned long var;
    unsigned int hi, lo;

    asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
    var = ((unsigned long long int) hi << 32) | lo;

    return var;
}

static inline void cpu_pause()
{
    __asm__ volatile ("pause" ::: "memory");
}

static inline void clwb(void *addr)
{
    cpu_pause();
}

static inline void pm_wbarrier(unsigned long lat)
{
    unsigned long etsc = read_tsc() + (unsigned long)(lat*CPU_FREQ_MHZ/1000);
    while (read_tsc() < etsc)
        cpu_pause();
}

#define DELAY_IN_NS (5000)

static inline void measure()
{
    unsigned long stsc, etsc;

    stsc = read_tsc();
    pm_wbarrier(DELAY_IN_NS);
    etsc = read_tsc();
    printf ("pm_wbarrier latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

    stsc = read_tsc();
    pm_wbarrier(2*DELAY_IN_NS);
    etsc = read_tsc();
    printf ("pm_wbarrier latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));

    stsc = read_tsc();
    clwb(NULL);
    etsc = read_tsc();
    printf ("clwb latency: %lu ns\n",
            (unsigned long)((etsc-stsc)*1000/CPU_FREQ_MHZ));
}

int main(void)
{
    measure();
    return 0;
}

