#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

guint tree_rotations = 0;
guint g_tree_rotations () {
    guint i = tree_rotations;
    tree_rotations = 0;
    return i;
}

static gint
cmp (gconstpointer k1, gconstpointer k2)
{
  glong r = GPOINTER_TO_UINT(k1) - GPOINTER_TO_UINT(k2);
  return r < 0 ? -1 : (r == 0 ? 0 : 1);
}

static void
time_normalize(struct timeval *len)
{
    while (len->tv_usec < 0) {
        --len->tv_sec;
        len->tv_usec += 1000000;
    }
    while (len->tv_usec >= 1000000) {
        ++len->tv_sec;
        len->tv_usec -= 1000000;
    }
}

static void
time_distance(struct timeval start, struct timeval end, struct timeval *len)
{
    len->tv_sec = end.tv_sec - start.tv_sec;
    len->tv_usec = end.tv_usec - start.tv_usec;

    time_normalize(len);
}

int main(int argc, char **argv)
{
  struct timeval start, end, len;
  guint i, n, seed, *nums;
  GTree *tree;

  if (argc != 3) return 1;

  seed = atol(argv[1]);
  n = atol(argv[2]);

  nums = g_new(guint, n);
  g_random_set_seed (seed);
  for (i = 0; i < n; ++i)
      nums[i] = g_random_int();

  printf (" Single-Version Tree...\n");

  tree = g_tree_new (cmp);

  printf ("  Inserting %8d elements...", n);
  fflush (stdout);
  gettimeofday (&start, NULL);
  for (i = 0; i < n; ++i)
    g_tree_insert (tree, GUINT_TO_POINTER(nums[i]),
                   GUINT_TO_POINTER(nums[i]));
  gettimeofday (&end, NULL);
  time_distance (start, end, &len);
  printf ("done. %lu.%06lu seconds",
           (gulong)len.tv_sec, (gulong)len.tv_usec);
  printf (" %u rotations\n", g_tree_rotations ());

  printf ("  Performing %8d queries...", n*10);
  fflush (stdout);
  gettimeofday (&start, NULL);
  for (i = 0; i < n*10; ++i)
      g_tree_lookup (tree, GUINT_TO_POINTER(nums[i%n]+1));
  gettimeofday (&end, NULL);
  time_distance (start, end, &len);
  printf ("done. %lu.%06lu seconds",
           (gulong)len.tv_sec, (gulong)len.tv_usec);
  printf (" %u rotations\n", g_tree_rotations ());

  printf ("  Deleting  %8d elements...", n/2*2);
  fflush (stdout);
  gettimeofday (&start, NULL);
  for (i = 0; i < n/2; ++i)
    g_tree_remove (tree, GUINT_TO_POINTER(nums[i*2]));
  for (i = 0; i < n/2; ++i)
    g_tree_remove (tree, GUINT_TO_POINTER(nums[i*2+1]));
  gettimeofday (&end, NULL);
  time_distance (start, end, &len);
  printf ("done. %lu.%06lu seconds",
           (gulong)len.tv_sec, (gulong)len.tv_usec);
  printf (" %u rotations\n", g_tree_rotations ());

  printf ("  Destroying the tree...        ");
  fflush (stdout);
  gettimeofday (&start, NULL);
  g_tree_unref (tree);
  gettimeofday (&end, NULL);
  time_distance (start, end, &len);
  printf ("done. %lu.%06lu seconds",
           (gulong)len.tv_sec, (gulong)len.tv_usec);
  printf (" %u rotations\n", g_tree_rotations ());

  g_free (nums);

  return 0;
}
