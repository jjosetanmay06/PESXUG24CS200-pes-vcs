// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions: tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
// "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Forward declarations (from object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────
#define MODE_FILE 0100644
#define MODE_EXEC 0100755
#define MODE_DIR  0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    if (S_ISDIR(st.st_mode)) return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);
        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;
        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';
        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 for the null terminator written by sprintf
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out  = offset;
    return 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Recursive helper: given a slice of index entries (all sharing a common
// path prefix of length `prefix_len`), build a tree object and write it.
//
// For example, if prefix = "src/" (prefix_len=4), entries look like:
//   "src/main.c", "src/util.c"
// We strip the prefix and treat what's left as names at this level.
static int write_tree_level(IndexEntry *entries, int count,
                             const char *prefix, size_t prefix_len,
                             ObjectID *id_out)
{
    Tree tree;
    tree.count = 0;

    int i = 0;
    while (i < count) {
        // rel = path relative to current level (after stripping prefix)
        const char *rel = entries[i].path + prefix_len;
        const char *slash = strchr(rel, '/');

        if (slash == NULL) {
            // ── Direct file at this level ──────────────────────────────────
            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = entries[i].mode;
            strncpy(e->name, rel, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
            e->hash = entries[i].hash;
            i++;
        } else {
            // ── Subdirectory ──────────────────────────────────────────────
            // Extract the directory name component
            size_t dir_name_len = (size_t)(slash - rel);
            char dir_name[256];
            memcpy(dir_name, rel, dir_name_len);
            dir_name[dir_name_len] = '\0';

            // Build new prefix = prefix + dir_name + "/"
            size_t new_prefix_len = prefix_len + dir_name_len + 1;
            char new_prefix[1024];
            memcpy(new_prefix, prefix, prefix_len);
            memcpy(new_prefix + prefix_len, dir_name, dir_name_len);
            new_prefix[prefix_len + dir_name_len] = '/';
            new_prefix[new_prefix_len] = '\0';

            // Find end of this subdirectory's entries (contiguous since sorted)
            int j = i;
            while (j < count &&
                   strncmp(entries[j].path, new_prefix, new_prefix_len) == 0) {
                j++;
            }

            // Recursively write subtree for these entries
            ObjectID sub_id;
            if (write_tree_level(entries + i, j - i,
                                  new_prefix, new_prefix_len, &sub_id) != 0) {
                return -1;
            }

            // Add a directory entry pointing to the subtree
            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = 0040000;
            strncpy(e->name, dir_name, sizeof(e->name) - 1);
            e->name[sizeof(e->name) - 1] = '\0';
            e->hash = sub_id;

            i = j; // Skip past all entries handled by the subtree
        }
    }

    // Serialize this tree level and write it to the object store
    void *raw;
    size_t raw_len;
    if (tree_serialize(&tree, &raw, &raw_len) != 0) return -1;
    int rc = object_write(OBJ_TREE, raw, raw_len, id_out);
    free(raw);
    return rc;
}

// Build a tree hierarchy from the current index and write all tree objects.
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    static Index index;
    memset(&index, 0, sizeof(index));
    // index_load returns 0 even for an empty index (file doesn't exist yet)
    if (index_load(&index) != 0) return -1;

    // Handle the empty index case (initial commit with no files)
    if (index.count == 0) {
        Tree empty;
        empty.count = 0;
        void *raw;
        size_t raw_len;
        if (tree_serialize(&empty, &raw, &raw_len) != 0) return -1;
        int rc = object_write(OBJ_TREE, raw, raw_len, id_out);
        free(raw);
        return rc;
    }

    // Build the full tree starting from the root level (empty prefix)
    return write_tree_level(index.entries, index.count, "", 0, id_out);
}
