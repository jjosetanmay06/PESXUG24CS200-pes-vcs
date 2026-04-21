// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions: index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declarations
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("    staged: %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("    (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("    deleted: %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size  != (off_t)index->entries[i].size) {
                printf("    modified: %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("    (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("    untracked: %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("    (nothing to show)\n");
    printf("\n");
    return 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Comparator for qsort — sorts index entries alphabetically by path
static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path,
                  ((const IndexEntry *)b)->path);
}

// Load the index from .pes/index.
// If the file does not exist, initializes an empty index (not an error).
// Returns 0 on success, -1 on read error.
int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // File doesn't exist yet — that's fine, empty index
        return 0;
    }

    char line[2048];
    while (fgets(line, sizeof(line), f)) {
        // Strip trailing newline
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;
        if (index->count >= MAX_INDEX_ENTRIES) break;

        IndexEntry *e = &index->entries[index->count];
        char hex[HASH_HEX_SIZE + 1];

        // Format: <mode> <hex> <mtime> <size> <path>
        unsigned int mode;
        unsigned long long mtime;
        unsigned int size;
        char path[512];

        if (sscanf(line, "%o %64s %llu %u %511s",
                   &mode, hex, &mtime, &size, path) == 5) {
            if (hex_to_hash(hex, &e->hash) == 0) {
                e->mode      = (uint32_t)mode;
                e->mtime_sec = (uint64_t)mtime;
                e->size      = (uint32_t)size;
                strncpy(e->path, path, sizeof(e->path) - 1);
                e->path[sizeof(e->path) - 1] = '\0';
                index->count++;
            }
        }
    }

    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically using temp file + rename.
// Sorts entries by path before writing.
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // Make a mutable copy so we can sort without modifying the caller's data
    Index sorted = *index;
    qsort(sorted.entries, sorted.count, sizeof(IndexEntry), compare_index_entries);

    // Write to a temp file next to the real index
    char tmp_path[] = INDEX_FILE ".tmp";
    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;

    for (int i = 0; i < sorted.count; i++) {
        const IndexEntry *e = &sorted.entries[i];
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&e->hash, hex);
        // Format: <mode-octal> <hex> <mtime> <size> <path>
        fprintf(f, "%o %s %llu %u %s\n",
                e->mode, hex,
                (unsigned long long)e->mtime_sec,
                (unsigned int)e->size,
                e->path);
    }

    // Flush userspace buffers, then fsync to disk
    fflush(f);
    fsync(fileno(f));
    fclose(f);

    // Atomically replace the real index
    if (rename(tmp_path, INDEX_FILE) != 0) {
        unlink(tmp_path);
        return -1;
    }
    return 0;
}

// Stage a file for the next commit.
// Reads the file, writes it as a blob to the object store, and records
// its hash + metadata in the index.
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    // Read file contents
    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long file_size_l = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (file_size_l < 0) { fclose(f); return -1; }
    size_t file_size = (size_t)file_size_l;

    void *contents = NULL;
    if (file_size > 0) {
        contents = malloc(file_size);
        if (!contents) { fclose(f); return -1; }
        if (fread(contents, 1, file_size, f) != file_size) {
            free(contents); fclose(f); return -1;
        }
    }
    fclose(f);

    // Write blob to the object store
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, contents ? contents : "", file_size, &blob_id) != 0) {
        free(contents);
        return -1;
    }
    free(contents);

    // Get file metadata for change detection
    struct stat st;
    if (stat(path, &st) != 0) return -1;

    // Determine file mode
    uint32_t mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;

    // Update or insert the index entry
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        // Update in place
        existing->hash      = blob_id;
        existing->mode      = mode;
        existing->mtime_sec = (uint64_t)st.st_mtime;
        existing->size      = (uint32_t)st.st_size;
    } else {
        // New entry
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index is full\n");
            return -1;
        }
        IndexEntry *e = &index->entries[index->count++];
        e->hash      = blob_id;
        e->mode      = mode;
        e->mtime_sec = (uint64_t)st.st_mtime;
        e->size      = (uint32_t)st.st_size;
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
    }

    return index_save(index);
}
